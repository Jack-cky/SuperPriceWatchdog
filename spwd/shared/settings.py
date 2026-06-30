import configparser
import os
from pathlib import Path

import pytz
from dotenv import load_dotenv


BASE_DIR = Path(__file__).parent.parent.parent

load_dotenv(BASE_DIR / "config" / ".env")

INI = configparser.ConfigParser()
INI.read(BASE_DIR / "config" / "config.ini")


class Settings:
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")

    API_SECRET = os.getenv("API_SECRET")

    RETRY_DELAY = INI.get("SCHEDULER", "RETRY_DELAY")
    RETRY_COUNT = INI.get("SCHEDULER", "RETRY_COUNT")

    DELTA = INI.getint("TASK", "DELTA")
    THRESHOLD = INI.getfloat("TASK", "THRESHOLD")

    _time_zone = INI.get("TIME", "TIMEZONE")
    HKT = pytz.timezone(_time_zone)


class Endpoints:
    _fw_url = os.getenv("FORWARDING_URL")
    _tg_token = os.getenv("TELEGRAM_TOKEN")

    BOT = INI.get("API", "BOT").format(_fw_url)
    FILE = INI.get("API", "FILE")
    VERSION = INI.get("API", "VERSION")

    IMG = INI.get("TELEGRAM", "IMG").format(_tg_token)
    MSG = INI.get("TELEGRAM", "MSG").format(_tg_token)
    WEBHOOK = INI.get("TELEGRAM", "WEBHOOK").format(_tg_token, _fw_url)
