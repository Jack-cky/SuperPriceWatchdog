import logging
import re
from datetime import datetime, timedelta
from io import BytesIO

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import requests
from flask import Blueprint, current_app, request

from ..models.messages import BotMessages


bp = Blueprint("response", __name__)

matplotlib.use("agg")


def slash_start(usr_id: int, user_name: str, usr_lang: str) -> str:
    response = current_app.supabase_client.rpc(
        "register_user",
        {"usr_id": usr_id, "usr_lang": usr_lang},
    ).execute()

    return BotMessages.start(
        user_name,
        response.data[0].get("_language"),
        response.data[0].get("_status"),
        slash_unk("na"),
    )


def slash_help(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "get_language",
        {"usr_id": usr_id},
    ).execute()

    return BotMessages.help(
        response.data[0].get("_language"),
        slash_unk("na"),
    )


def slash_list(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "get_watchlist",
        {"usr_id": usr_id},
    ).execute()

    items = [
        f"/{data['_sku']} | {data['_brand']} - {data['_name']}"
        for data in response.data
    ]

    return "🛒 🛒 🛒 🛒 🛒\n\n" + "\n".join(items) if items else slash_unk("na")


def slash_sub(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "change_subscription",
        {"usr_id": usr_id},
    ).execute()

    return BotMessages.sub(
        response.data[0].get("_language"),
        response.data[0].get("_status"),
        slash_unk("na"),
    )


def slash_lucky(usr_id: int) -> str:
    year, week, weekday = datetime.now(current_app.hkt).isocalendar()

    if weekday not in [5, 6]:
        response = current_app.supabase_client.rpc(
            "draw_deals",
            {"usr_id": usr_id},
        ).execute()

        items = []
        for data in response.data:
            price = f"${data['_price']:.1f}\n" if abs(data["_fix"] - data["_price"]) <= 0.1 else f"<s>${data['_fix']:.1f}</s> → ${data['_price']:.1f} ({data['_promotion']})\n"
            items.append(
                f"<a href='https://online-price-watch.consumer.org.hk/opw/product/{data['_sku']}'>{data['_sku']}</a> {data['_brand']} - {data['_name']}\n"
                f"{data['_supermarket']} @ {price}"
                f"<span class='tg-spoiler'>MA({data['_frequency']}): ${data['_average']:.1f} ± {data['_std']:.1f} | min. ${data['_q0']:.1f} | max. ${data['_q4']:.1f}</span>"
            )

        msg = f"🍀 {year} WK{week} Day {(weekday + 1) % 7} 🍀\n\n" \
            + "\n\n".join(items) if items else slash_unk("na")
    else:
        response = current_app.supabase_client.rpc(
            "get_language",
            {"usr_id": usr_id},
        ).execute()

        msg = BotMessages.lucky(
            response.data[0].get("_language"),
            slash_unk("na"),
        )

    return msg


def slash_lang(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "change_language",
        {"usr_id": usr_id},
    ).execute()

    return BotMessages.lang(
        response.data[0].get("_language"),
        slash_unk("na"),
    )


def slash_alert(usr_id: int) -> str | None:
    response = current_app.supabase_client.rpc(
        "get_alert",
        {"usr_id": usr_id},
    ).execute()

    items = []
    for data in response.data:
        price = f"${data['_price']:.1f}\n" if abs(data["_fix"]-data["_price"]) <= 0.1 else f"<s>${data['_fix']:.1f}</s> → ${data['_price']:.1f} ({data['_promotion']})\n"
        items.append(
            f"/{data['_sku']} | {data['_brand']} - {data['_name']}\n"
            f"{data['_supermarket']} @ {price}"
        )

    today = datetime.now(current_app.hkt)
    year, week, weekday = today.isocalendar()
    dates = [(today+timedelta(days=delta)).day for delta in range((4-weekday)%7+1)]

    special_offer = ""

    n_days = [idx for idx, date in enumerate(dates) if date in [3, 13, 23]]  # Wellcome
    if n_days:
        if n_days[0] == 0:
            special_offer += "\n💳 Wellcome enJoy Card 8% off: TODAY."
        else:
            special_offer += f"\n💳 Wellcome enJoy Card 8% off: {n_days[0]} more day(s)."

    n_days = [idx for idx, date in enumerate(dates) if date in [2, 12, 22]]  # ParknShop
    if n_days:
        if n_days[0] == 0:
            special_offer += "\n💳 ParknShop Platinum Card 8% off: TODAY."
        else:
            special_offer += f"\n💳 ParknShop Platinum Card 8% off: {n_days[0]} more day(s)."

    return f"📢 {year} WK{week} Day {(weekday + 1) % 7} 📢\n\n" + \
        "\n".join(items) + special_offer if items else None


def slash_edit(usr_id: int, code: str) -> str:
    response = current_app.supabase_client.rpc(
        "edit_watchlist",
        {"usr_id": usr_id, "code": code},
    ).execute()

    return BotMessages.edit(
        response.data[0].get("_language"),
        response.data[0].get("_status"),
        response.data[0].get("_valid"),
        slash_unk("na"),
    )


def slash_error(status: str) -> str:
    return BotMessages.error(
        status,
        slash_unk("na"),
    )


def slash_plot(usr_id: int, code: str) -> bytes:
    response = current_app.supabase_client.rpc(
        "get_prices",
        {"code": code},
    ).execute()

    x = mdates.datestr2num([data["_date"] for data in response.data])
    y = [data["_price"] for data in response.data]

    response = current_app.supabase_client.rpc(
        "get_item",
        {"usr_id": usr_id, "code": code},
    ).execute()

    ref = response.data[0]["_bid"]
    n_day = response.data[0]["_frequency"]
    brand = response.data[0]["_brand"]
    prod = response.data[0]["_name"]

    plt.figure(figsize=(11, 4))
    plt.plot(x, y, label="Price")
    plt.axhline(y=ref, color="r", linestyle="--", label="Target")

    plt.title(
        f"{n_day:.0f} Days Price Trend for {brand}; {prod}",
        loc="left",
        fontproperties=current_app.font,
    )
    plt.title(
        f"As of {datetime.now(current_app.hkt).strftime('%Y-%m-%d %H:%M:%S')}",
        loc="right",
        fontsize=8,
        style="italic",
    )
    plt.xlabel("Date")
    plt.ylabel("Price (HKD)")
    plt.legend()

    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=7))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d %b"))

    img = BytesIO()
    plt.savefig(img, format="png")
    plt.close()
    img.seek(0)

    return img.getvalue()


def slash_bye(usr_id: int) -> str:
    response = current_app.supabase_client.rpc(
        "remove_user",
        {"usr_id": usr_id},
    ).execute()

    return BotMessages.bye(
        response.data[0].get("_language"),
        slash_unk("na"),
    )


def slash_unk(status: str) -> str:
    return BotMessages.unk(status)


def get_command(usr_msg: str) -> tuple[str, str]:
    usr_input = re.findall(r"/opw/product/(.*)", usr_msg)

    if usr_input:
        code = usr_input[0]
        slash = "/edit"
    else:
        code = ""
        slash = usr_msg.split()[0]

    if re.search(r"/P?\d+", slash):
        code = slash[1:]
        slash = "/plot"

    return code, slash


def send_response(usr_id: str, msg: str, img: bytes | None) -> None:
    try:
        if img:
            params = {
                "url": current_app.config["API_IMG"],
                "data": {
                    "chat_id": usr_id,
                },
                "files": {
                    "photo": img,
                },
            }
        else:
            params = {
                "url": current_app.config["API_MSG"],
                "data": {
                    "chat_id": usr_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": 1,
                },
            }

        requests.post(**params, timeout=30)
    except requests.RequestException:
        logging.error(f"Failed to reply {usr_id}'s message:", exc_info=True)


@bp.route("/api/v1/reply", methods=["POST"])
def handle_message() -> str:
    """
    /start  greet the user and register them
    /help   provide a help message with instructions for the user
    /list   retrieve and list all items the user is currently tracking
    /sub    update the user's subscription status for daily alerts
    /lucky  get a list of randomly selected best deals for the day
    /lang   change the user's preferred language for responses
    /alert  get a list of best deals for the day
    /plot   generate a time series plot for a specific item's price trends
    /edit   update the user's watchlist by adding or removing items
    /error  generate an error message based on the provided status
    /bye    unregister the user and provide a farewell message
    """
    data = request.get_json()

    if "message" in data:
        usr_id = data["message"]["from"]["id"]
        usr_msg = data["message"]["text"]

        msg, img = "", None
        try:
            code, slash = get_command(usr_msg)

            match slash:
                # external slashs
                case "/start":
                    usr_name = data["message"]["from"]["first_name"]
                    usr_lang = data["message"]["from"]["language_code"]
                    msg = slash_start(usr_id, usr_name, usr_lang)
                case "/help":
                    msg = slash_help(usr_id)
                case "/list":
                    msg = slash_list(usr_id)
                case "/sub":
                    msg = slash_sub(usr_id)
                case "/lucky":
                    msg = slash_lucky(usr_id)
                case "/lang":
                    msg = slash_lang(usr_id)
                # internal slashs
                case "/alert":
                    msg = slash_alert(usr_id)
                case "/edit":
                    msg = slash_edit(usr_id, code)
                case "/error":
                    msg = slash_error("pipeline")
                case "/plot":
                    img = slash_plot(usr_id, code)
                # hidden slashs
                case "/bye":
                    msg = slash_bye(usr_id)
                # default reply
                case _:
                    msg = slash_unk("unk")
        except:
            msg = slash_error("user")
            slash = "/error"

            logging.error(f"Failed to parse {usr_id} command:", exc_info=True)
        finally:
            send_response(usr_id, msg, img)

            logging.info(f"Message ({usr_id}): {usr_msg}")
    else:
        logging.warning("No message found in the request.")

        return "", 400

    return "", 200
