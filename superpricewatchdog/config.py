import configparser
import os
from pathlib import Path

from dotenv import load_dotenv


PTH = Path(__file__).parent.parent

load_dotenv(PTH / "config" / ".env")

CONFIG = configparser.ConfigParser()
CONFIG.read(PTH / "config" / "config.ini")


class Config:
    GITHUB_SECRET = os.getenv("GITHUB_SECRET")

    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SCHEMA = CONFIG.get("DATABASE", "SCHEMA")

    _fw_url = os.getenv("FORWARDING_URL")
    _tg_token = os.getenv("TELEGRAM_TOKEN")
    API_IMG = CONFIG.get("TELEGRAM", "IMG").format(_tg_token)
    API_MSG = CONFIG.get("TELEGRAM", "MSG").format(_tg_token)
    API_WEBHOOK = CONFIG.get("TELEGRAM", "WEBHOOK").format(_tg_token, _fw_url)

    TIMEZONE = CONFIG.get("TIME", "TIMEZONE")
