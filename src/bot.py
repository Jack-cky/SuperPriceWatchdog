"""
SuperPriceWatchdog Bot for handling Telegram bot interactions. This web
application processes user messages, queries data from a Supabase database, and
sends formatted responses back to users. The bot is designed to operate as a
webhook, allowing it to respond  to incoming messages and perform actions based
on user commands.
"""
import logging
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from superpricewatchdog import create_app


app = create_app()

logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

for library in ["httpx", "werkzeug"]:
    logging.getLogger(library).setLevel(logging.WARNING)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
