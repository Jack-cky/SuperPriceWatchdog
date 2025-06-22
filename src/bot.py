"""
SuperPriceWatchdog is a web application designed to handle Telegram bot
interactions and monitor price changes using Open Price Watch (OPW) data. The
application processes user messages through a webhook, querying data from a
Supabase database and sending back formatted responses based on user commands.
Additionally, it features an ETLT (Extract, Transform, Load, Transform) pipeline
that runs daily to manage price data. This pipeline downloads, cleanses, and
updates the database with new prices and items, alerting users to any
significant price changes.
"""
import logging

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
