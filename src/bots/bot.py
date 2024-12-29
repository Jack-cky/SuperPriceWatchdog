import hashlib
import hmac
import logging
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
from flask import Flask, render_template, request
from supabase import create_client
from supabase.client import ClientOptions


load_dotenv("./SuperPriceWatchdog/config/.env")

logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
for library in ["httpx", "werkzeug"]:
    logging.getLogger(library).setLevel(logging.WARNING)

matplotlib.use("agg")
plt.rcParams["font.family"] = "AR PL Mingti2L Big5"


class Config:
    """
    Configuration class to manage environment variables and constants.
    """
    FORWARDING_URL = os.getenv("FORWARDING_URL")
    GITHUB_SECRET = os.getenv("GITHUB_SECRET")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")
    TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

    API_BASE = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"
    API_SEND_MSG = f"{API_BASE}/sendMessage"
    API_SEND_IMG = f"{API_BASE}/sendPhoto"
    API_WEBHOOK = f"{API_BASE}/setWebhook?url={FORWARDING_URL}/api/v1/reply"

    HKT = pytz.timezone("Asia/Singapore")


class SuperPriceWatchdogBot(Config):
    """
    SuperPriceWatchdogBot class for handling Telegram bot interactions.
    This class processes user messages, queries data from a Supabase 
    database, and sends formatted responses back to users.

    The bot is designed to operate as a webhook, allowing it to respond 
    to incoming messages and perform actions based on user commands.
    """
    def __init__(self, app: Flask):
        self.app = app
        self.supabase_client = create_client(
            self.SUPABASE_URL,
            self.SUPABASE_KEY,
            ClientOptions(schema=self.SUPABASE_SCHEMA),
        )

        self._register_routes()

        requests.get(self.API_WEBHOOK, timeout=30)

    def _register_routes(self) -> None:
        """Register API routes."""
        self.app.route("/")(self.index)
        self.app.route("/api/v1/deploy", methods=["POST"])(self.git_pull)
        self.app.route("/api/v1/reply", methods=["POST"])(self.handle_message)

    def index(self) -> str:
        """Render a page from the GitHub README."""
        return render_template("index.html")

    def _validate_signature(self, signature: str, data: bytes) -> bool:
        """Validate the GitHub webhook signature."""
        hash_algorithm, github_signature = signature.split("=", 1)
        algorithm = hashlib.__dict__.get(hash_algorithm)
        encoded_key = bytes(self.GITHUB_SECRET, "latin-1")
        mac = hmac.new(encoded_key, msg=data, digestmod=algorithm)

        return hmac.compare_digest(mac.hexdigest(), github_signature)

    def git_pull(self) -> str:
        """Pull the latest code from the GitHub repository."""
        signature = request.headers.get("X-Hub-Signature")

        if signature and self._validate_signature(signature, request.data):
            try:
                repo = git.Repo("./SuperPriceWatchdog")
                origin = repo.remotes.origin

                repo.git.reset("--hard", "HEAD")
                repo.git.clean("-fdX")

                repo.create_head("main", origin.refs.main) \
                    .set_tracking_branch(origin.refs.main) \
                    .checkout()

                origin.pull()
            except Exception as e:
                logging.error(f"Failed to deploy from the latest code: {e}")

                return "", 500
        else:
            logging.warning("Invalid Github webhook signature.")

            return "", 403

        return "", 200

    def _get_command(self, usr_msg: str) -> tuple[str, str]:
        """Parse the user's message to extract the command and code."""
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

    def _send_response(self, usr_id: str, msg: str, img: bytes | None) -> None:
        """Send a response message or image to the user."""
        try:
            if img:
                params = {
                    "url": self.API_SEND_IMG,
                    "data": {
                        "chat_id": usr_id,
                    },
                    "files": {
                        "photo": img,
                    },
                }
            else:
                params = {
                    "url": self.API_SEND_MSG,
                    "data": {
                        "chat_id": usr_id,
                        "text": msg,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": 1,
                    },
                }

            requests.post(**params, timeout=30)
        except requests.RequestException as e:
            logging.error(f"Failed to reply {usr_id}'s message: {e}")

    def _slash_start(self, usr_id: int, user_name: str, usr_lang: str) -> str:
        """Greet the user and register them."""
        response = self.supabase_client.rpc(
            "register_user",
            {"usr_id": usr_id, "usr_lang": usr_lang},
        ).execute()

        language = response.data[0].get("_language")
        status = response.data[0].get("_status")

        msg = {
            ("en", "new"): (
                f"Good day {user_name}! I'm SuperPriceWatchdog!\n\n"
                "I always keep my eyes on supermarket promotions and I'll alert you whenever your favourite items are on fantastic deals.\n\n"
                "Let's start our savings journey! 🐶"
            ),
            ("en", "repeat"): f"How come you /start again, {user_name}? Use /help for the user guide.",
            ("zh", "new"): (
                f"哈囉{user_name}！我喺超市看門狗啊！\n\n"
                "我會幫你昅實超市啲優惠，同埋我會同提你幾時可以入手平價貨。\n\n"
                "一齊嚟屠宰啲超市喇！ 🐶"
            ),
            ("zh", "repeat"): f"你又 /start 乜春嘢啊{user_name}？唔知點用就 /help 喇。",
        }

        return msg.get((language, status), self._slash_unknown("na"))

    def _slash_help(self, usr_id: int) -> str:
        """Provide a help message with instructions for the user."""
        response = self.supabase_client.rpc(
            "get_language",
            {"usr_id": usr_id},
        ).execute()

        language = response.data[0].get("_language")

        msg = {
            "en": (
                "🔰 Helping you, helping you\n\n"
                "Go to this [<a href='https://online-price-watch.consumer.org.hk/opw/category'>website</a>] to find an item you're interested in, then copy the link and send it to me. I'll help you keep track of the price trends for that item.\n\n"
                "Easy peasy! Suppose you want to buy a specific type of soft drink, just send me the link. Try sending me this link:\nhttps://online-price-watch.consumer.org.hk/opw/product/P000000002\n"
                "If you want to stop monitoring it, just send me the link again.\n\n"
                "I'll add the product you want to track to this /list, and you can view and modify the items in it anytime. Remember to /sub for daily price alerts, so when the item is at a good price, I'll remind you to buy it.\n\n"
                "If you feel a bit lost, you can try feeling /lucky and randomly see what's on sale today. Also, you can 更改 your /lang to 中文 if you 唔識睇.\n\n"
                "If you want to give feedback or find any bugs, you can leave a message for us to improve at this [<a href='https://github.com/Jack-cky/SuperPriceWatchdog/issues'>link</a>]."
            ),
            "zh": (
                "🔰 幫緊你，幫緊你\n\n"
                "去依個[<a href='https://online-price-watch.consumer.org.hk/opw/category'>網站</a>]到搵你想關注嘅產品，然後copy條link俾我，我就會幫你留意件貨嘅價錢趨勢㗎喇。\n\n"
                "好簡單！譬如你想飲某牌子嘅汽水，咁你只需要搵條到link俾我就得㗎喇。試下copy依條link俾我：\nhttps://online-price-watch.consumer.org.hk/opw/product/P000000002\n"
                "如果唔想我再留意件貨，send多次條link俾我就得㗎喇。\n\n"
                "我會將你想關注嘅產品放入依條 /list 裏面，你隨時都可以睇返同修改入面嘅嘢。記住 /sub 每日嘅價格通知，咁到時件貨抵買嘅時間我就會提你入手㗎喇。\n\n"
                "如果你覺得迷惘，你可以試下 feeling /lucky 咁隨機睇吓今日有啲乜嘢抵買。仲有你可以讕喺嘢咁change你個 /lang 去English。\n\n"
                "如果你想發表意見或者發現有bugs，你可以去[<a href='https://github.com/Jack-cky/SuperPriceWatchdog/issues'>依到</a>]留個言俾我地去改善㗎。"
            ),
        }

        return msg.get(language, self._slash_unknown("na"))

    def _slash_list(self, usr_id: int) -> str:
        """Retrieve and list all items the user is currently tracking."""
        response = self.supabase_client.rpc(
            "get_watchlist",
            {"usr_id": usr_id},
        ).execute()

        items = [
            f"/{data['_sku']} | {data['_brand']} - {data['_name']}"
            for data in response.data
        ]

        headline = "🛒 🛒 🛒 🛒 🛒\n\n"

        return headline + "\n".join(items) if items else self._slash_unknown("na")

    def _slash_sub(self, usr_id: int) -> str:
        """Update the user's subscription status for daily alerts."""
        response = self.supabase_client.rpc(
            "change_subscription",
            {"usr_id": usr_id},
        ).execute()

        language = response.data[0].get("_language")
        status = response.data[0].get("_status")

        msg = {
            ("en", "y"): "Woof! Let's embark on our saving journey! Every day (except Friday) around <u><b>12:00</b></u>, I'll bark you the latest deals!",
            ("en", "n"): "Sad to see you go.",
            ("zh", "y"): "汪！係時候屠宰啲超市喇！每日(除週五)大概<u><b>12:00</b></u>我就會吠醒你最抵買嘅平價貨！",
            ("zh", "n"): "遺棄看門狗因住俾愛協打。",
        }

        return msg.get((language, status), self._slash_unknown("na"))

    def _slash_lucky(self, usr_id: int) -> str:
        """Get a list of randomly selected best deals for the day."""
        calendar = datetime.now(self.HKT).isocalendar()

        if calendar.weekday not in [5, 6]:
            response = self.supabase_client.rpc(
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

            headline = f"🍀 {calendar.year} WK{calendar.week} Day {(calendar.weekday + 1) % 7} 🍀\n\n"

            return headline + "\n\n".join(items) if items else self._slash_unknown("na")
        else:
            response = self.supabase_client.rpc(
                "get_language",
                {"usr_id": usr_id},
            ).execute()

            language = response.data[0].get("_language")

            msg = {
                "en": (
                    "🍀 🍀 🍀 🍀 🍀\n\n"
                    "Data is being updated. It's not lucky on Fridays and Saturday."
                ),
                "zh": (
                    "🍀 🍀 🍀 🍀 🍀\n\n"
                    "數據更新中，逢週五六唔lucky。"
                )
            }

            return msg.get(language, self._slash_unknown("na"))

    def _slash_lang(self, usr_id: int) -> str:
        """Change the user's preferred language for responses."""
        response = self.supabase_client.rpc(
            "change_language",
            {"usr_id": usr_id},
        ).execute()

        language = response.data[0].get("_language")

        msg = {
            "en": "Changed to English language.",
            "zh": "幫你轉咗做中文。",
        }

        return msg.get(language, self._slash_unknown("na"))

    def _slash_alert(self, usr_id: int) -> str:
        """Get a list of items currently on alert for best deals."""
        response = self.supabase_client.rpc(
            "get_alert",
            {"usr_id": usr_id},
        ).execute()

        items = []
        for data in response.data:
            price = f"${data['_price']:.1f}\n" if abs(data["_fix"] - data["_price"]) <= 0.1 else f"<s>${data['_fix']:.1f}</s> → ${data['_price']:.1f} ({data['_promotion']})\n"
            items.append(
                f"/{data['_sku']} | {data['_brand']} - {data['_name']}\n"
                f"{data['_supermarket']} @ {price}"
            )

        calendar = datetime.now(self.HKT).isocalendar()
        headline = f"📢 {calendar.year} WK{calendar.week} Day {(calendar.weekday + 1) % 7} 📢\n\n"

        return headline + "\n".join(items) if items else None

    def _slash_edit(self, usr_id: int, code: str) -> str:
        """Update the user's watchlist by adding or removing items."""
        response = self.supabase_client.rpc(
            "edit_watchlist",
            {"usr_id": usr_id, "code": code},
        ).execute()

        language = response.data[0].get("_language")
        status = response.data[0].get("_status")
        valid = response.data[0].get("_valid")

        msg = {
            ("en", "add", True): "Roger! I'm staring at this item.",
            ("en", "remove", True): "Sure! I'll no long monitor this item.",
            ("en", "na", False): "This item doesn't exist. Double check it.",
            ("zh", "add", True): "收到！會睇實依件貨啲價格趨勢。",
            ("zh", "remove", True): "可以！唔會再留意依件貨。",
            ("zh", "na", False): "無依件貨㗎喎，睇清楚啲。",
        }

        return msg.get((language, status, valid), self._slash_unknown("na"))

    def _slash_error(self, status: str) -> str:
        """Generate an error message based on the provided status."""
        msg = {
            "pipeline": "[Error 500] Internal Error. Data Pipeline appeared to have issues.",
            "user": (
                "[Error 500] Internal Error. It has been recorded in the system log.\n\n"
                "If the problem if the problem persists, please create an issue <a href='https://github.com/Jack-cky/SuperPriceWatchdog/issues'>here</a>."
            ),
        }

        return msg.get(status, self._slash_unknown("na"))

    def _slash_plot(self, usr_id: int, code: str) -> bytes:
        """Generate a time series plot for a specific item's price trends."""
        response = self.supabase_client.rpc(
            "get_prices",
            {"code": code},
        ).execute()

        x = mdates.datestr2num([data["_date"] for data in response.data])
        y = [data["_price"] for data in response.data]

        response = self.supabase_client.rpc(
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

        return img.getvalue()

    def _slash_bye(self, usr_id: int) -> str:
        """Unregister the user and provide a farewell message."""
        response = self.supabase_client.rpc(
            "remove_user",
            {"usr_id": usr_id},
        ).execute()

        language = response.data[0].get("_language")

        msg = {
            "en": "You know the secret! /start again to restart everything.",
            "zh": "咁你都知有依樣嘢喎。你要 /start 多次我先幫到你。",
        }

        return msg.get(language, self._slash_unknown("na"))

    def _slash_unknown(self, status: str) -> str:
        """Generate a default response message for unrecognised commands."""
        msg = {
            "unk": "🐶 bow-wow 汪 ~ 🐶 bow-wow 汪 ~",
            "na": "🚬 ૮´˶• ᴥ •˶`ა 🥃",
        }

        return msg.get(status, "na")

    def handle_message(self) -> str:
        """Process incoming user messages and dispatch commands."""
        data = request.get_json()

        if "message" in data:
            usr_id = data["message"]["from"]["id"]
            usr_msg = data["message"]["text"]
            logging.info(f"Message ({usr_id}): {usr_msg}")

            try:
                code, slash = self._get_command(usr_msg)
                msg, img = "", None

                match slash:
                    # external slashs
                    case "/start":
                        usr_name = data["message"]["from"]["first_name"]
                        usr_lang = data["message"]["from"]["language_code"]
                        msg = self._slash_start(usr_id, usr_name, usr_lang)
                    case "/help":
                        msg = self._slash_help(usr_id)
                    case "/list":
                        msg = self._slash_list(usr_id)
                    case "/sub":
                        msg = self._slash_sub(usr_id)
                    case "/lucky":
                        msg = self._slash_lucky(usr_id)
                    case "/lang":
                        msg = self._slash_lang(usr_id)
                    # internal slashs
                    case "/alert":
                        msg = self._slash_alert(usr_id)
                    case "/edit":
                        msg = self._slash_edit(usr_id, code)
                    case "/error":
                        msg = self._slash_error("pipeline")
                    case "/plot":
                        img = self._slash_plot(usr_id, code)
                    # hidden slashs
                    case "/bye":
                        msg = self._slash_bye(usr_id)
                    # default reply
                    case _:
                        msg = self._slash_unknown("unk")
            except Exception as e:
                msg = self._slash_error("user")
                slash = "/error"
                logging.error(f"Failed to parse {usr_id} command: {e}")
            finally:
                self._send_response(usr_id, msg, img)
        else:
            logging.warning("No message found in the request.")

            return "", 400

        return "", 200


PTH_TEMPLATE = os.path.join(os.path.dirname(__file__), "../../templates")
app = Flask(__name__, template_folder=PTH_TEMPLATE)
bot = SuperPriceWatchdogBot(app)


if __name__ == "__main__":
    app.run(port=5000, debug=True)
