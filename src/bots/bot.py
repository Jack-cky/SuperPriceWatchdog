import hashlib
import hmac
import os
import re
from datetime import datetime
from io import BytesIO

import git
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pytz
import requests
from dotenv import load_dotenv
from flask import Flask, request
from supabase import create_client
from supabase.client import ClientOptions


load_dotenv("./SuperPriceWatchdog/config/.env")


class Config:
    FORWARDING_URL = os.getenv("FORWARDING_URL")
    GITHUB_SECRET = os.getenv("GITHUB_SECRET")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
    
    API_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    API_SEND_MSG = f"{API_BASE}/sendMessage"
    API_SEND_IMG = f"{API_BASE}/sendPhoto"
    URL_OPW = "https://online-price-watch.consumer.org.hk/opw"
    
    HKT = pytz.timezone("Asia/Singapore")

app = Flask(__name__)
app.config.from_object(Config)

matplotlib.use("agg")
plt.rcParams["font.family"] = "AR PL Mingti2L Big5"

CLIENT = create_client(
    app.config["SUPABASE_URL"],
    app.config["SUPABASE_KEY"],
    ClientOptions(schema=app.config["SUPABASE_SCHEMA"]),
)

requests.get(
    f"{app.config['API_BASE']}/setWebhook"
    f"?url={app.config['FORWARDING_URL']}/api/v1/reply"
)



@app.route("/api/v1/deploy", methods=["POST"])
def git_pull():
    def is_valid_signature(x_hub_signature, data, private_key):
        hash_algorithm, github_signature = x_hub_signature.split("=", 1)
        algorithm = hashlib.__dict__.get(hash_algorithm)
        encoded_key = bytes(private_key, "latin-1")
        mac = hmac.new(encoded_key, msg=data, digestmod=algorithm)
        
        return hmac.compare_digest(mac.hexdigest(), github_signature)
    
    x_hub_signature = request.headers.get("X-Hub-Signature")

    if is_valid_signature(x_hub_signature, request.data, app.config["GITHUB_SECRET"]):
        repo = git.Repo("./SuperPriceWatchdog")
        origin = repo.remotes.origin
        repo.git.reset("--hard", "HEAD")
        repo.git.clean("-fdX")
        repo.create_head("main", origin.refs.main).set_tracking_branch(origin.refs.main).checkout()
        origin.pull()
    
    return "", 200


@app.route("/")
def index():
    
    return (
        "SuperPriceWatchdog is in service right now! "
        "Check out <a href='https://t.me/SuperPriceWatchdogBot'>HERE</a>."
    )


def slash_start(usr_id: int, user_name: str, usr_lang: str) -> str:
    response = CLIENT.rpc(
        "register_user",
        {"usr_id": usr_id, "usr_lang": usr_lang},
    ).execute()
    
    language = response.data[0].get("_language")
    status = response.data[0].get("_status")
    
    match language, status:
        case "en", "new":
            msg = (
                f"Good day {user_name}! I'm SuperPriceWatchdog!\n\n"
                "I always keep my eyes on supermarket promotions and I'll alert "
                "you whenever your favourite items are on fantastic deals.\n\n"
                "Let's start our savings journey!"
            )
        case "en", "repeat":
            msg = (
                f"How come you /start again, {user_name}? "
                "Use /help for the user guide."
            )
        case "zh", "new":
            msg = (
                f"哈囉{user_name}！我喺超市看門狗啊！\n\n"
                "我會幫你昅實超市啲優惠，同埋我會同提你幾時可以入手平價貨。\n\n"
                "一齊嚟屠宰啲超市喇！"
            )
        case "zh", "repeat":
            msg = f"你又 /start 乜春嘢啊{user_name}？唔知點用就 /help 喇。"
    
    return msg


def slash_help(usr_id: int) -> str:
    response = CLIENT.rpc(
        "get_language",
        {"usr_id": usr_id},
    ).execute()
    
    language = response.data[0].get("_language")
    
    match language:
        case "en":
            msg = (
                "helping you, helping you\n"
                "====================\n"
                "/help....read user guide\n"
                "/list.......review tracking list\n"
                "/lucky...check random items on best deal\n"
                "/sub.....subscribe to daily pricing alert\n"
                "/lang....轉返做中文咪幾好\n"
                "====================\n"
                "Go and visit this "
                f"<a href='{app.config['URL_OPW']}/category'>website</a> "
                "to find your target items. Copy the hyperlink and send it to "
                "me. Then I'll help you to monitor their prices.\n\n"
                "Easy peasy! Suppose you wanna buy a specific type of soft "
                "drink, send the link to me. Try and send me this link:\n"
                f"{app.config['URL_OPW']}/product/P000000002\n\n"
                "If you wanna forget it, send me the link again."
            )
        case "zh":
            msg = (
                "幫緊你，幫緊你\n"
                "====================\n"
                "/help....睇用戶指南\n"
                "/list.......睇吓有啲乜留意緊\n"
                "/lucky...隨機睇吓今日有啲乜嘢抵買\n"
                "/sub.....訂閲每日價格通知\n"
                "/lang....讕喺嘢switch to English\n"
                "====================\n"
                f"去依個<a href='{app.config['URL_OPW']}/category'>網站</a>"
                "到搵你想關注嘅貨品，然後copy條link俾我，"
                "我就會幫你留意件貨嘅價錢趨勢㗎喇。\n\n"
                "好簡單！譬如你想飲某牌子嘅汽水，咁你只需要搵條到link俾我就得㗎喇。"
                "試下copy依條link俾我：\n"
                f"{app.config['URL_OPW']}/product/P000000002\n\n"
                "如果唔想我再留意件貨，send多次條link俾我就得㗎喇。"
            )
        case "na":
            msg = slash_unknown("na")
    
    return msg


def slash_list(usr_id: int) -> str:
    response = CLIENT.rpc(
        "get_watchlist",
        {"usr_id": usr_id},
    ).execute()
    
    items = []
    for data in response.data:
        item = f"/{data['_sku']} | {data['_brand']} - {data['_name']}"
        items.append(item)
    
    headline = (
        f"🛒 🛒 🛒 🛒 🛒\n"
        "================\n"
    )
    
    return headline + "\n".join(items) if items else slash_unknown("na")


def slash_lucky(usr_id: int) -> str:
    response = CLIENT.rpc(
        "get_random_deals",
        {"usr_id": usr_id},
    ).execute()
    
    items = []
    for data in response.data:
        item = (
            f"<a href='{app.config['URL_OPW']}/product/{data['_sku']}'>"
            f"{data['_sku']}"
            "</a> "
            f"{data['_brand']} - {data['_name']}\n"
            f"{data['_supermarket']} @ "
            f"${data['_price']:.1f} ({data['_promotion']})\n"
            "<span class='tg-spoiler'>"
            f"MA({data['_frequency']}): "
            f"${data['_average']:.1f} ± {data['_std']:.1f} | "
            f"min. ${data['_q0']:.1f} | max. ${data['_q4']:.1f}"
            "</span>"
        )
        items.append(item)
    
    headline = (
        f"🍀 {datetime.now(app.config['HKT']).strftime('%Y-%m-%d')} 🍀\n"
        "===============\n"
    )
    
    return headline + "\n\n".join(items) if items else slash_unknown("na")


def slash_sub(usr_id: int) -> str:
    response = CLIENT.rpc(
        "change_subscription",
        {"usr_id": usr_id},
    ).execute()
    
    language = response.data[0].get("_language")
    status = response.data[0].get("_status")
    
    match language, status:
        case "en", "y":
            msg = (
                "Wooo! Let's embark on our saving journey! "
                "Every day (except Friday) around <u><b>12:00</b></u>, "
                "I'll bark you the latest deals!"
            )
        case "en", "n":
            msg = "Sad to see you go."
        case "zh", "y":
            msg = (
                "汪！係時候屠宰啲超市喇！"
                "每日(除週五)大概<u><b>12:00</b></u>我就會吠醒你最抵買嘅平價貨！"
            )
        case "zh", "n":
            msg = "遺棄看門狗因住俾愛協打。"
        case "na", _:
            msg = slash_unknown("na")
    
    return msg


def slash_lang(usr_id: int) -> str:
    response = CLIENT.rpc(
        "change_language",
        {"usr_id": usr_id},
    ).execute()
    
    language = response.data[0].get("_language")
    
    match language:
        case "en":
            msg = "Changed to English language."
        case "zh":
            msg = "幫你轉咗做中文。"
        case "na":
            msg = slash_unknown("na")
    
    return msg


def slash_alert(usr_id: int) -> str:
    response = CLIENT.rpc(
        "get_alert",
        {"usr_id": usr_id},
    ).execute()
    
    items = []
    for data in response.data:
        item = (
            f"/{data['_sku']} | {data['_brand']} - {data['_name']}\n"
            f"{data['_supermarket']} @ "
            f"<s>${data['_fix']:.1f}</s> → ${data['_price']:.1f}\n"
            f"{data['_promotion']}"
        )
        
        items.append(item)
    
    headline = (
        f"📢 {datetime.now(app.config['HKT']).strftime('%Y-%m-%d')} 📢\n"
        "================\n"
    )
    
    return headline + "\n\n".join(items) if items else None


def slash_edit(usr_id: int, code: str) -> str:
    response = CLIENT.rpc(
        "edit_watchlist",
        {"usr_id": usr_id, "code": code},
    ).execute()
    
    language = response.data[0].get("_language")
    status = response.data[0].get("_status")
    valid = response.data[0].get("_valid")
    
    match language, status, valid:
        case "en", "add", True:
            msg = "Roger! I'm staring at this item."
        case "en", "remove", True:
            msg = "Sure! I'll no long monitor this item."
        case "en", _, False:
            msg = "This item doesn't exist. Double check it."
        case "zh", "add", True:
            msg = "收到！會睇實依件貨啲價格趨勢。"
        case "zh", "remove", True:
            msg = "可以！唔會再留意依件貨。"
        case "zh", _, False:
            msg = "無依件貨㗎喎，睇清楚啲。"
        case "na", _, _:
            msg = slash_unknown("na")
    
    return msg


def slash_plot(usr_id: int, code: str) -> bytes:
    response = CLIENT.rpc(
        "get_prices",
        {"code": code},
    ).execute()
    
    x = mdates.datestr2num([data["_date"] for data in response.data])
    y = [data["_price"] for data in response.data]
    
    response = CLIENT.rpc(
        "get_item",
        {"usr_id": usr_id, "code": code},
    ).execute()
    
    ref = response.data[0]["_q1"]
    n_day = response.data[0]["_frequency"]
    brand = response.data[0]["_brand"]
    prod = response.data[0]["_name"]
    
    plt.figure(figsize=(11, 4))
    plt.plot(x, y, label="Price")
    plt.axhline(y=ref, color="r", linestyle="--", label="Bid")
    
    plt.title(f"{n_day:.0f} Days Price Trend for {brand}; {prod}")
    plt.xlabel("Date")
    plt.ylabel("Price (HKD)")
    plt.legend()
    
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b %y"))
    
    img = BytesIO()
    plt.savefig(img, format="png")
    plt.close()
    img.seek(0)
    
    image_bytes = img.getvalue()
    
    return image_bytes


def slash_bye(usr_id: int) -> str:
    response = CLIENT.rpc(
        "remove_user",
        {"usr_id": usr_id},
    ).execute()
    
    language = response.data[0].get("_language")
    
    match language:
        case "en":
            msg = "You know the secret! /start again to restart everything."
        case "zh":
            msg = "咁你都知有依樣嘢喎。你要 /start 多次我先幫到你。"
        case "na":
            msg = slash_unknown("na")
    
    return msg


def slash_unknown(status: str) -> str:
    match status:
        case "unk":
            msg = "🐶 bow-wow 汪 ~ 🐶 bow-wow 汪 ~"
        case "na":
            msg = "🚬 ૮´˶• ᴥ •˶`ა 🥃"
    
    return msg


def slash_error(status: str) -> str:
    match status:
        case "user":
            msg = (
                "[Error 500] Internal Error. It has been reported to the "
                "development team.\n\n If the problem if the problem persists, "
                "please create an issue"
                "<a href='https://github.com/Jack-cky/SuperPriceWatchdog/issues'> here"
                "</a>."
            )
        case "developer":
            msg = (
                "TODO"
            )
    
    return msg


@app.route("/api/v1/reply", methods=["POST"])
def SuperPriceWatchdogBot():
    data = request.get_json()
    
    if "message" in data:
        usr_id = data["message"]["from"]["id"]
        usr_msg = data["message"]["text"]
        
        print(f"[MESSAGE] {usr_id} {usr_msg}")
        
        usr_input = re.findall(r"/opw/product/(.*)", usr_msg)
        if usr_input:
            code = usr_input[0]
            slash = "/edit"
        else:
            slash = usr_msg.split()[0]
        
        if re.search(r"\d+", slash):
            code = slash[1:]
            slash = "/plot"
        
        try:
            match slash:
                # expected slashs
                case "/start":
                    usr_name = data["message"]["from"]["first_name"]
                    usr_lang = data["message"]["from"]["language_code"]
                    msg = slash_start(usr_id, usr_name, usr_lang)
                case "/help":
                    msg = slash_help(usr_id)
                case "/list":
                    msg = slash_list(usr_id)
                case "/lucky":
                    msg = slash_lucky(usr_id)
                case "/sub":
                    msg = slash_sub(usr_id)
                case "/lang":
                    msg = slash_lang(usr_id)
                # internal slashs
                case "/alert":
                    msg = slash_alert(usr_id)
                case "/edit":
                    msg = slash_edit(usr_id, code)
                case "/plot":
                    img = slash_plot(usr_id, code)
                # hidden slashs
                case "/bye":
                    msg = slash_bye(usr_id)
                # default reply
                case _:
                    msg = slash_unknown("unk")
        except:
            slash = "/error"
        
        match slash:
            case "/error":
                msg = slash_error("user")
                data = {
                    "chat_id": usr_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": 1,
                }
                requests.post(app.config["API_SEND_MSG"], data)
                
                # msg = slash_error("developer")
                # data = {
                #     "chat_id": "TODO",
                #     "text": f"error on {usr_id}",
                #     "parse_mode": "HTML",
                #     "disable_web_page_preview": 1,
                # }
                # requests.post(app.config["API_SEND_MSG"], data)
            case "/plot":
                data = {"chat_id": usr_id}
                files = {"photo": img}
                requests.post(app.config["API_SEND_IMG"], data, files=files)
            case _:
                data = {
                    "chat_id": usr_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": 1,
                }
                requests.post(app.config["API_SEND_MSG"], data)
    
    return "", 200


if __name__ == "__main__":
    app.run(port=5000)
