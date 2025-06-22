import configparser
import logging
import os
from pathlib import Path

import luigi
from dotenv import load_dotenv


PTH = Path(__file__).parent.parent

load_dotenv(PTH / "config" / ".env")

CONFIG = configparser.ConfigParser()
CONFIG.read(PTH / "config" / "config.ini")

for config in [
    ("worker", "keep-alive", "True"),
    ("worker", "no_install_shutdown_handler", "True"),
    ("scheduler", "retry_delay", CONFIG.get("SCHEDULER", "RETRY_DELAY")),
    ("scheduler", "retry_count", CONFIG.get("SCHEDULER", "RETRY_COUNT")),
]:
    luigi.configuration.get_config().set(*config)

LOGGER = logging.getLogger("luigi-interface")


class Config:
    SECRET_GITHUB = os.getenv("SECRET_GITHUB")
    SECRET_PIPELINE = os.getenv("SECRET_PIPELINE")

    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")

    _fw_url = os.getenv("FORWARDING_URL")
    _tg_token = os.getenv("TELEGRAM_TOKEN")

    API_BOT = CONFIG.get("API", "BOT").format(_fw_url)
    API_FILE = CONFIG.get("API", "FILE")
    API_VERSION = CONFIG.get("API", "VERSION")

    DELTA = CONFIG.getint("TASK", "DELTA")
    THRESHOLD = CONFIG.getfloat("TASK", "THRESHOLD")

    API_IMG = CONFIG.get("TELEGRAM", "IMG").format(_tg_token)
    API_MSG = CONFIG.get("TELEGRAM", "MSG").format(_tg_token)
    API_WEBHOOK = CONFIG.get("TELEGRAM", "WEBHOOK").format(_tg_token, _fw_url)

    HOUR = CONFIG.getint("TIME", "HOUR")
    TIMEZONE = CONFIG.get("TIME", "TIMEZONE")
