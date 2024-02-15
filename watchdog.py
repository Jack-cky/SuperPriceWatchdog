import logging
import os
import re
from datetime import time

from dotenv import load_dotenv
from matplotlib import dates as mdates, pyplot as plt
from matplotlib.font_manager import FontProperties
from pytz import timezone
from tabulate import tabulate
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, 
    CommandHandler, 
    ContextTypes, 
    filters,
    MessageHandler
)

from src import *


## CONFIGURATION
# -----

if os.path.isfile("./configs/.env"):
    load_dotenv("./configs/.env")
IMG_PTH = os.getenv("IMG_PTH")
FONT_PTH = os.getenv("FONT_PTH")
VERBOSE = bool(os.getenv("VERBOSE"))
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

if os.path.isfile(FONT_PTH):
    plt_font = FontProperties(fname=FONT_PTH)
else:
    plt_font = None
    plt.rcParams["font.family"] = "Hiragino Maru Gothic Pro"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s\n%(message)s",
    level=logging.INFO,
)


## HELPER FUNCTIONS
# -----

def log_usr_act(func):
    def wrap(*args, **kwargs):
        chat = args[0].effective_chat
        message = args[0].effective_message
        
        msg = f"""
        {chat.first_name} ({chat.id}) | {func.__name__} | {message.text}
        """
        logging.info(tabulate([[msg]], tablefmt="fancy_grid"))
        
        res = func(*args, **kwargs)
        
        return res
    
    return wrap


def get_usr_info(usr:dict) -> tuple:
    uid = usr.id
    name = usr.first_name
    lang = query_database(
        f"""
        SELECT lang
        FROM t_user
        WHERE uid = {uid};
        """
    )["lang"].values
    lang = lang[0] if len(lang) else "NA"
    
    return uid, name, lang


def split_msg(msg:str, msg_len:int=3_000) -> list:
    msgs = []
    msg_parsa = ""
    for sentence in msg.split('\n'):
        if len(msg_parsa + sentence) <= msg_len:
            msg_parsa += f"\n{sentence}"
        else:
            msgs.append(msg_parsa)
            msg_parsa = ""
    msgs.append(msg_parsa)
    
    return msgs


def generate_trend_img(args:tuple) -> str:
    sku, x, y, ref, n_day, brand, prod = args
    pth = f"./{IMG_PTH}/{sku}.png"
    
    plt.figure(figsize=(11, 4))
    plt.plot(x, y, label="Price")
    plt.axhline(y=ref, color="r", linestyle="--", label="Bid")
    
    plt.title(
        f"{n_day:.0f} Days Price Trend for {brand}; {prod}",
        fontproperties=plt_font,
    )
    plt.xlabel("Date")
    plt.ylabel("Price (HKD)")
    plt.legend()
    
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.gcf().autofmt_xdate()
    
    plt.savefig(pth)
    
    return pth


async def watchdog(context: ContextTypes.DEFAULT_TYPE) -> None:
    job = context.job
    usr_id, usr_lang = job.data
    refresh_database()
    
    watch_lst = query_database(
        f"""
        SELECT
            brand_{usr_lang} AS brand, name_{usr_lang} AS name
            , price, promo_{usr_lang} AS promo
            , smkt
            , freq, price_avg, price_std, price_q0, price_q4
        FROM t_tracker
            LEFT JOIN t_watchdog
                ON t_watchdog.sku = t_tracker.sku
            LEFT JOIN t_item
                ON t_item.sku = t_tracker.sku
        WHERE 1 = 1
            AND uid = '{usr_id}'
            AND alert = 1;
        """
    ).to_dict("records")
    
    layer = ["INVALID", "WATCHDOG"][bool(len(watch_lst))]
    
    msg = get_msg_watchdog(usr_lang, watch_lst, layer)
    await context.bot.send_message(chat_id=job.chat_id, text=msg)
    
    return None


## TELEGRAM USER FUNCTIONS
# -----

@log_usr_act
async def slash_start(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, usr_name, usr_lang = get_usr_info(update.effective_chat)
    layer = "REPEAT"
    
    if usr_lang == "NA":
        layer = "FIRST_TIME"
        usr_lang = update.message.from_user.language_code
        usr_lang = ["en", usr_lang][usr_lang in ["en", "zh"]]
        
        execute_sql(
            f"""
            INSERT INTO t_user (uid, lang)
            VALUES ({usr_id}, '{usr_lang}');
            """
        )
    
    msg = get_msg_start(usr_lang, usr_name, layer)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_view(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    _, _, usr_lang = get_usr_info(update.effective_chat)
    usr_qry = "".join(context.args) \
        .upper().replace(" ", "")
    layer = "UNKNOWN"
    idx_dept = idx_cat = 0
    dept = cat = "ALL"
    item_lst = ["LIST_OF_SKU"]
    
    if re.search(r"^D\d+C\d+$", usr_qry):
        layer = "ITEM"
        idx_dept, idx_cat = re.findall(r"D(\d+)C(\d+)", usr_qry)[0]
        
        dept = query_database(
            f"""
            SELECT dept_{usr_lang} AS dept
            FROM (
                SELECT dept_{usr_lang}
                    , ROW_NUMBER() OVER (ORDER BY dept_{usr_lang}) AS rnk
                FROM t_item
                GROUP BY dept_{usr_lang}
            )
            WHERE rnk = {idx_dept};
            """
        )["dept"].values
        dept = "".join(dept)
        
        cat = query_database(
            f"""
            SELECT cat_{usr_lang} AS cat
            FROM (
                SELECT cat_{usr_lang}
                    , ROW_NUMBER() OVER (ORDER BY cat_{usr_lang}) AS rnk
                FROM t_item
                WHERE dept_{usr_lang} = '{dept}'
                GROUP BY cat_{usr_lang}
            )
            WHERE rnk = {idx_cat};
            """
        )["cat"].values
        cat = "".join(cat)
        
        item_lst = query_database(
            f"""
            SELECT sku || ' - '
                || brand_{usr_lang} || ' - '
                || name_{usr_lang} AS item_lst
            FROM t_item
            WHERE 1 = 1
                AND dept_{usr_lang} = '{dept}'
                AND cat_{usr_lang} = '{cat}'
                AND EXISTS (
                    SELECT 1
                    FROM t_price
                    WHERE t_price.sku = t_item.sku
                )
            ORDER BY sku;
            """
        )["item_lst"].values
    elif re.search(r"^D\d+$", usr_qry):
        layer = "CATEGORY"
        idx_dept = re.findall(r"D(\d+)", usr_qry)[0]
        
        dept = query_database(
            f"""
            SELECT dept_{usr_lang} AS dept
            FROM (
                SELECT dept_{usr_lang}
                    , ROW_NUMBER() OVER (ORDER BY dept_{usr_lang}) AS rnk
                FROM t_item
                GROUP BY dept_{usr_lang}
            )
            WHERE rnk = {idx_dept};
            """
        )["dept"].values
        dept = "".join(dept)
        
        item_lst = query_database(
            f"""
            SELECT '[C' || row_number() OVER (ORDER BY cat_{usr_lang}) || '] '
                || cat_{usr_lang} AS item_lst
            FROM t_item
            WHERE dept_{usr_lang} = '{dept}'
            GROUP BY cat_{usr_lang};
            """
        )["item_lst"].values
    elif usr_qry == "":
        layer = "DEPARTMENT"
        
        item_lst = query_database(
            f"""
            SELECT '[D' || ROW_NUMBER() OVER (ORDER BY dept_{usr_lang}) || '] '
                || dept_{usr_lang} AS item_lst
            FROM t_item
            GROUP BY dept_{usr_lang};
            """
        )["item_lst"].values
    
    view = {
        "idx": (idx_dept, idx_cat),
        "struct": (dept, cat),
        "item_lst": item_lst,
    }
    
    msg_str = get_msg_view(usr_lang, view, layer)
    msgs = split_msg(msg_str)
    for msg in msgs:
        await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_plot(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    usr_qry = "".join(context.args) \
        .upper().replace(" ", "")
    layer, img = ["UNKNOWN", "INVALID"][bool(len(usr_qry))], None
    
    if re.search(r"^P\d+$", usr_qry):
        sku_ts = query_database(
            f"""
            SELECT date
                , MIN(price) AS price
            FROM t_price
            WHERE sku = '{usr_qry}'
            GROUP BY date;
            """
        )
        sku_ts["date"] = pd.to_datetime(sku_ts["date"], format="%Y%m%d")
        
        if len(sku_ts):
            layer = "GRAPH"
            
            sku_info = query_database(
                f"""
                SELECT
                    price_q1, freq
                    , brand_{usr_lang} AS brand, name_{usr_lang} AS name
                FROM t_watchdog
                    LEFT JOIN t_item
                        ON t_item.sku = t_watchdog.sku
                WHERE t_watchdog.sku = '{usr_qry}';
                """
            ).to_dict("records")[0]
            
            img_args = (
                usr_qry,
                sku_ts["date"], sku_ts["price"], sku_info["price_q1"],
                sku_info["freq"], sku_info["brand"], sku_info["name"],
            )
            img_pth = generate_trend_img(img_args)
            img = open(img_pth, "rb")
    
    msg = get_msg_plot(usr_lang, layer)
    await update.effective_message.reply_text(msg)
    await context.bot.send_photo(chat_id=usr_id, photo=img) if img else None
    
    del img
    return None


@log_usr_act
async def slash_add(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    usr_qry = ",".join(f"'{sku}'" for sku in context.args) \
        .upper().replace(" ", "")
    layer, cnt = ["UNKNOWN", "INVALID"][bool(len(usr_qry))], 0
    
    if re.search(r"^'P\d+'(,'P\d+')*$", usr_qry):
        sku_pndg = query_database(
            f"""
            SELECT sku
            FROM t_item
            WHERE sku IN ({usr_qry});
            """
        )["sku"].values
        
        if len(sku_pndg):
            layer = "ADD"
            
            sku_str = ", ".join(f"'{sku.upper()}'" for sku in sku_pndg)
            sku_exst = query_database(
                f"""
                SELECT sku
                FROM t_tracker
                WHERE 1 = 1
                    AND uid = {usr_id}
                    AND sku IN ({sku_str});
                """
            )["sku"].values
            sku_lst = set(sku_pndg) - set(sku_exst)
            
            cnt = len(sku_lst)
            if cnt:
                for sku in sku_lst:
                    execute_sql(
                        f"""
                        INSERT INTO t_tracker (uid, sku)
                        VALUES ({usr_id}, '{sku}');
                        """
                    )
            else:
                layer = "EXIST"
    
    msg = get_msg_add(usr_lang, cnt, layer)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_rm(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    usr_qry = ",".join(f"'{sku}'" for sku in context.args) \
        .upper().replace(" ", "")
    layer, cnt = ["UNKNOWN", "INVALID"][bool(len(usr_qry))], 0
    
    if re.search(r"^'P\d+'(,'P\d+')*$", usr_qry):
        sku_pndg = query_database(
            f"""
            SELECT sku
            FROM t_item
            WHERE sku IN ({usr_qry});
            """
        )["sku"].values
        
        if len(sku_pndg):
            layer = "REMOVE"
            
            sku_str = ", ".join(f"'{sku.upper()}'" for sku in sku_pndg)
            sku_lst = query_database(
                f"""
                SELECT sku
                FROM t_tracker
                WHERE 1 = 1
                    AND uid = '{usr_id}'
                    AND sku IN ({sku_str});
                """
            )["sku"].values
            
            cnt = len(sku_lst)
            if cnt:
                sku_str = ", ".join(f"'{sku.upper()}'" for sku in sku_lst)
                execute_sql(
                    f"""
                    DELETE FROM t_tracker
                    WHERE 1 = 1
                        AND uid = {usr_id}
                        AND sku IN ({sku_str});
                    """
                )
            else:
                layer = "ABSENT"
    elif usr_qry == "'ALL'":
        layer = "EMPTY"
        
        execute_sql(
            f"""
            DELETE FROM t_tracker
            WHERE uid = {usr_id};
            """
        )
    
    msg = get_msg_rm(usr_lang, cnt, layer)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_list(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    
    items = query_database(
        f"""
        SELECT sku || ' - ' 
            || brand_{usr_lang} || ' - '
            || name_{usr_lang} AS item
        FROM t_item
        WHERE EXISTS (
            SELECT 1
            FROM t_tracker
            WHERE 1 = 1
                AND t_tracker.sku = t_item.sku
                AND uid = '{usr_id}'
        )
        ORDER BY sku;
        """
    )["item"].values
    
    msg = get_msg_list(usr_lang, items)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_watch(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    _, _, usr_lang = get_usr_info(update.effective_chat)
    
    watch_lst = query_database(
        f"""
        SELECT
            brand_{usr_lang} AS brand, name_{usr_lang} AS name
            , price, promo_{usr_lang} AS promo
            , smkt
            , freq, price_avg, price_std, price_q0, price_q4
        FROM t_watchdog
            LEFT JOIN t_item
                ON t_item.sku = t_watchdog.sku
        WHERE alert = 1;
        """
    ).sample(5).to_dict("records")
    
    layer = ["INVALID", "WATCHDOG"][bool(len(watch_lst))]
    
    msg = get_msg_watchdog(usr_lang, watch_lst, layer)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_sub(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    layer = "INVALID"
    
    current_jobs = context.job_queue.get_jobs_by_name(str(usr_id))
    if not current_jobs:
        layer = "SUBSCRIBE"
        msg_time = time(12, 30, tzinfo=timezone("Asia/Singapore"))
        context.job_queue.run_daily(
            watchdog, msg_time,
            chat_id=usr_id, name=str(usr_id),
            data=(usr_id, usr_lang),
        )
    
    msg = get_msg_sub(usr_lang, layer)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_unsub(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    layer = "INVALID"
    
    current_jobs = context.job_queue.get_jobs_by_name(str(usr_id))
    if current_jobs:
        layer = "UNSUBSCRIBE"
        for job in current_jobs:
            job.schedule_removal()
    
    msg = get_msg_unsub(usr_lang, layer)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_lang(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    usr_lang = ["en", "zh"][usr_lang == "en"]
    
    execute_sql(
        f"""
        UPDATE t_user
        SET lang = '{usr_lang}'
        WHERE uid = {usr_id};
        """
    )
    
    msg = get_msg_lang(usr_lang)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def slash_bye(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    usr_id, _, usr_lang = get_usr_info(update.effective_chat)
    
    execute_sql(
        f"""
        DELETE FROM t_tracker
        WHERE uid = {usr_id};
        """
    )
    
    execute_sql(
        f"""
        DELETE FROM t_user
        WHERE uid = {usr_id};
        """
    )
    
    current_jobs = context.job_queue.get_jobs_by_name(str(usr_id))
    if current_jobs:
        for job in current_jobs:
            job.schedule_removal()
    
    msg = get_msg_bye(usr_lang)
    await update.effective_message.reply_text(msg)
    
    return None

## TELEGRAM NON USER FUNCTIONS
# -----

@log_usr_act
async def unknown(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    _, _, usr_lang = get_usr_info(update.effective_chat)
    
    msg = get_msg_unknown(usr_lang)
    await update.effective_message.reply_text(msg)
    
    return None


@log_usr_act
async def reply(update:Update, context:ContextTypes.DEFAULT_TYPE) -> None:
    _, _, usr_lang = get_usr_info(update.effective_chat)
    
    msg = get_msg_reply(usr_lang)
    await update.effective_message.reply_text(msg)
    
    return None


## MAIN FUNCTION
# -----

def main() -> None:
    set_up_backend()
    refresh_opw_data()
    
    tg_bot = ApplicationBuilder() \
        .token(TELEGRAM_TOKEN).build()
    
    for var, func in dict(globals()).items():
        if var.startswith("slash_") and callable(func):
            slash = var.replace("slash_", "")
            hder = CommandHandler(slash, func)
            tg_bot.add_handler(hder)
    
    hder = MessageHandler(filters.COMMAND, unknown)
    tg_bot.add_handler(hder)
    
    hder = MessageHandler(filters.TEXT & (~filters.COMMAND), reply)
    tg_bot.add_handler(hder)
    
    tg_bot.run_polling()
    
    return None


if __name__ == "__main__":
    main()
