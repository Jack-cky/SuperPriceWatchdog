import json
import logging
import os
import re
from datetime import datetime, timedelta

import polars as pl
import pytz
import requests
from dotenv import load_dotenv
from supabase import create_client
from supabase.client import ClientOptions


load_dotenv("./SuperPriceWatchdog/config/.env")

logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logging.getLogger("httpx").setLevel(logging.WARNING)


class Config:
    """
    Configuration class to manage environment variables and constants.
    """
    DELTA = int(os.getenv("DELTA"))
    DEVELOPER_ID = os.getenv("DEVELOPER_ID")
    FORWARDING_URL = os.getenv("FORWARDING_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    SUPABASE_SCHEMA = os.getenv("SUPABASE_SCHEMA")
    SUPABASE_URL = os.getenv("SUPABASE_URL")

    API_BASE = "https://api.data.gov.hk/v1/historical-archive"
    API_SOURCE = "https://online-price-watch.consumer.org.hk/opw/opendata/pricewatch.json"
    API_VERSION = f"{API_BASE}/list-file-versions?url={API_SOURCE}&start={{}}&end={{}}"
    API_FILE = f"{API_BASE}/get-file?url={API_SOURCE}&time={{}}"
    API_BOT = f"{FORWARDING_URL}/api/v1/reply"

    DT_END = datetime.today() - timedelta(days=1)
    HKT = pytz.timezone("Asia/Singapore")

    DISCOUNT_THRESHOLD = 0.3
    BATCH_VERSION = 10
    BATCH_INSERT = 10_000


class SuperPriceWatchdogPipeline(Config):
    """
    SuperPriceWatchdogPipeline class for monitoring price changes using Open
    Price Watch (OPW) data. This class implements an ETL (Extract, Transform,
    Load) process to manage price data and alert users of any price changes.

    The pipeline is designed to run daily, downloading price data, cleansing it,
    and updating a database with new prices and items.
    """
    def __init__(self):
        self.supabase_client = create_client(
            self.SUPABASE_URL,
            self.SUPABASE_KEY,
            ClientOptions(schema=self.SUPABASE_SCHEMA),
        )

        self.date_expiry = []
        self.date_version = {}

        self.df_item = pl.DataFrame()
        self.df_price = pl.DataFrame()

    def _fetch_opw_versions(self) -> None:
        """Get available OPW file versions and update windowing period."""
        versions = []
        for length in range(0, self.DELTA, self.BATCH_VERSION):
            end = self.DT_END - timedelta(days=length)
            start = end - timedelta(days=9)
            dates = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

            response = requests.get(self.API_VERSION.format(*dates), timeout=20)
            response.raise_for_status()

            data = response.json()
            versions += data.get("timestamps", [])

        self.date_version.update(dict(
            pl.DataFrame({"version": versions})
            .with_columns(
                pl.col("version").str.slice(0, 8)
                    .str.to_date("%Y%m%d")
                    .alias("date")
            )
            .with_columns(
                (pl.col("date") - pl.duration(days=1))  # OPW data is a day delayed
                    .dt.strftime("%Y%m%d")
            )
            .select("date", "version")
            .iter_rows()
        ))

        response = self.supabase_client.rpc("get_dates").execute()

        for data in response.data:
            if data["_date"] in self.date_version:
                self.date_version.pop(data["_date"], None)
            else:
                self.date_expiry.append(data["_date"])

        logging.info(f"Window of {len(self.date_version)} day(s) to be updated.")

    def _download_opw_data(self) -> None:
        """Download and process pending OPW data into DataFrames."""
        prices, items = [], []

        for date, version in self.date_version.items():
            response = requests.get(self.API_FILE.format(version), timeout=20)
            response.raise_for_status()

            data = response.json()

            for item in data:
                item["code"] = str(item["code"]).upper()
                code = item["code"]

                price = item.pop("prices", [])
                offer = item.pop("offers", [])

                # expand sub-dictionaries into a single object
                smkt_price = {p["supermarketCode"]: p for p in price}
                smkt_offer = {o["supermarketCode"]: o for o in offer}

                price = [
                    {
                        "code": code, "date": date,
                        **smkt_price.get(smkt, {}), **smkt_offer.get(smkt, {}),
                    }
                    for smkt in set(smkt_price) | set(smkt_offer)
                ]

                prices += price
                items.append(item)

        self.df_item = pl.json_normalize(items).rechunk()
        self.df_price = pl.from_records(prices).rechunk()

        logging.info(f"Total of {len(items):,} raw item(s) and {len(prices):,} raw price(s).")

    def _cleanse_opw_data(self) -> None:
        """Cleanse OPW data to match the defined schema in the database."""
        if not self.df_item.is_empty():
            response = self.supabase_client.rpc("get_skus").execute()
            sku_list = [data["_sku"] for data in response.data]

            cols = {
                "code": "sku",
                "cat1Name.en": "department_en",
                "cat1Name.zh-Hant": "department_zh",
                "cat2Name.en": "category_en",
                "cat2Name.zh-Hant": "category_zh",
                "cat3Name.en": "subcategory_en",
                "cat3Name.zh-Hant": "subcategory_zh",
                "brand.en": "brand_en",
                "brand.zh-Hant": "brand_zh",
                "name.en": "name_en",
                "name.zh-Hant": "name_zh",
            }

            self.df_item = (
                self.df_item
                .unique(subset="code")  # keep unique SKUs
                .filter(~pl.col("code").is_in(sku_list))  # filter out existing SKUs
                .select(cols)
                .rename(cols)
                .rechunk()
            )

        if not self.df_price.is_empty():
            cols = {
                "code": "sku",
                "date": "effective_date",
                "supermarketCode": "supermarket",
                "en": "promotion_en",
                "zh-Hant": "promotion_zh",
                "price": "original_price",
            }

            self.df_price = (
                self.df_price
                .with_columns(
                    pl.col(["en", "zh-Hant"]).fill_null("No Promotion"),  # default no promotion
                    pl.col("price").str.extract(r"([\d\.]+)")
                        .cast(pl.Float32)
                        .fill_null(0),  # ensure a valid price for each record
                )
                .select(cols)
                .rename(cols)
                .rechunk()
            )

        logging.info(f"Total of {len(self.df_item):,} new item(s) and {len(self.df_price):,} new price(s).")

    def _split_promotion(self, mkt_txt: str) -> list[str]:
        """Clean and split promotion text into individual components."""
        txt = re.sub(r"\s?wk\d+\s?", "", mkt_txt.lower().strip())
        txt = re.sub(r"; |/|[a-z]\.[a-z]", "<sep>", txt)
        txt = re.sub(r"half price", "50%", txt)
        txt = re.sub(r"second", "2nd", txt)

        return list(map(str.strip, txt.split("<sep>")))

    def _get_pattern(self, promotion: str) -> str:
        """Map promotional text patterns to tags for later processing."""
        pattern_tags = {
            r"\$\d+(\.\d+)?": "<AMT>",
            r"\d+(\.\d+)?\%": "<PCT>",
            r"\d+": "<CNT>",
        }

        txt = promotion.lower().strip()

        for pat, tag in pattern_tags.items():
            txt = re.sub(pat, tag, txt)

        return txt

    def _extract_values(self, data: dict[str, str]) -> list[float]:
        """Extract numeric values from the promotion pattern."""
        values = []
        if data["category"]:
            promotions = data["promotion"].split()
            patterns = data["pattern"].split()
            for promotion, pattern in zip(promotions, patterns):
                if re.search(r"<AMT>|<CNT>|<PCT>", pattern):
                    nums = re.findall(r"(\d+\.?\d{,2})", promotion)
                    values += [float(num) for num in nums]

        return values

    def _calculate_discount(self, data: dict[str, float | list[float]]) -> float:
        """Calculate the discounted price based on the promotion rules."""
        price, pattern, category, values = data.values()
        discount = price

        try:
            match category:
                case 2:
                    if pattern == "+<AMT> for <CNT>nd item":
                        discount = price + values[0]
                        discount /= values[1]
                    elif pattern == "<AMT> for <CNT>":
                        discount = values[0] / values[1]
                case 4:
                    if re.search(r"<CNT>\s.*save" , pattern):
                        discount = price * values[0] - values[1]
                        discount /= values[0]
                    elif re.search(r"<CNT>\s" , pattern):
                        discount = values[1] / values[0]
                case 5:
                    if re.search(r"free the most expensive one" , pattern):
                        discount = price * (values[0] - 1)
                        discount /= values[0]
                    elif re.search(r"get <CNT> free" , pattern):
                        discount = price * values[0]
                        discount /= values[0] + values[1]
                case 6:
                    if re.search(r"<CNT>\w" , pattern):
                        discount = price * (values[0] - 1) \
                            + price * (1 - values[1] / 100)
                        discount /= values[0]
                    else:
                        discount = price * values[0] * (1 - values[1] / 100)
                        discount /= values[0]
                case 8:
                    if re.search(r"<CNT>\w" , pattern):
                        discount = price * (1 - values[0] / 100) \
                            + price * (values[1] - 1)
                        discount /= values[1]
                    else:
                        discount = price * (1 - values[0] / 100)
                case _:
                    discount = price
        except Exception:
            pass

        return discount if price * self.DISCOUNT_THRESHOLD < discount else price

    def _analyse_promotion(self) -> None:
        """Categorise promotions and calculate unit prices."""
        if not self.df_price.is_empty():
            self.df_price = (
                self.df_price
                .with_columns(
                    pl.col("promotion_en")
                        .map_elements(self._split_promotion, return_dtype=list[str])
                        .alias("promotion"),  # split description with multiple promotions
                )
                .explode("promotion")  # expand row per promotion
                .with_columns(
                    pl.col("promotion")
                        .map_elements(self._get_pattern, return_dtype=str)
                        .alias("pattern"),  # get <AMT>, <CNT> and <PCT> as pattern
                )
                .with_columns(
                    pl.col("pattern").str.count_matches("<AMT>").alias("amt"),
                    pl.col("pattern").str.count_matches("<CNT>").alias("cnt"),
                    pl.col("pattern").str.count_matches("<PCT>").alias("pct"),
                )
                .with_columns(
                    pl.when(  # NA
                        pl.col("pattern").str.contains(r"<AMT>.*<AMT>")
                            & (pl.col("amt") == 2)
                            & (pl.col("cnt") == 0)
                            & (pl.col("pct") == 0)
                    ).then(1)
                    .when(  # <$> for <n>
                        pl.col("pattern").str.contains(r"<AMT>.*<CNT>")
                            & (pl.col("amt") == 1)
                            & (pl.col("cnt") == 1)
                            & (pl.col("pct") == 0)
                    ).then(2)
                    .when(  # NA
                        pl.col("pattern").str.contains(r"<AMT>.*<PCT>")
                            & (pl.col("amt") == 1)
                            & (pl.col("cnt") == 0)
                            & (pl.col("pct") == 1)
                    ).then(3)
                    .when(  # buy <n> at/save <$>
                        pl.col("pattern").str.contains(r"<CNT>.*<AMT>")
                            & (pl.col("amt") == 1)
                            & (pl.col("cnt") == 1)
                            & (pl.col("pct") == 0)
                    ).then(4)
                    .when(  # buy <n> get <n> free
                        pl.col("pattern").str.contains(r"<CNT>.*<CNT>")
                            & (pl.col("amt") == 0)
                            & (pl.col("cnt") == 2)
                            & (pl.col("pct") == 0)
                    ).then(5)
                    .when(  # buy <n> at <%>
                        pl.col("pattern").str.contains(r"<CNT>.*<PCT>")
                            & (pl.col("amt") == 0)
                            & (pl.col("cnt") == 1)
                            & (pl.col("pct") == 1)
                    ).then(6)
                    .when(  # NA
                        pl.col("pattern").str.contains(r"<PCT>.*<AMT>")
                            & (pl.col("amt") == 1)
                            & (pl.col("cnt") == 0)
                            & (pl.col("pct") == 1)
                    ).then(7)
                    .when(  # <%> for <n>
                        pl.col("pattern").str.contains(r"<PCT>.*<CNT>")
                            & (pl.col("amt") == 0)
                            & (pl.col("cnt") == 1)
                            & (pl.col("pct") == 1)
                    ).then(8)
                    .when(  # NA
                        pl.col("pattern").str.contains(r"<PCT>.*<PCT>")
                            & (pl.col("amt") == 0)
                            & (pl.col("cnt") == 0)
                            & (pl.col("pct") == 2)
                    ).then(9)
                    .otherwise(0)
                    .alias("category"),  # categorise promotion into 9 groups
                )
                .with_columns(
                    pl.struct("promotion", "pattern", "category")
                        .map_elements(self._extract_values, return_dtype=list[float])
                        .alias("value"),  # get numeric values according to pattern
                )
                .with_columns(
                    pl.struct("original_price", "pattern", "category", "value")
                        .map_elements(self._calculate_discount, return_dtype=float)
                        .alias("unit_price"),  # calculate price based on defined rules
                )
                .sort(["sku", "effective_date", "supermarket", "unit_price"])
                .unique(subset=["sku", "effective_date", "supermarket"])
                .drop([
                    "amt", "cnt", "pct",
                    "promotion", "pattern", "category", "value",
                ])
                .rechunk()
            )

            cnt = (
                self.df_price
                .filter(pl.col("original_price") != pl.col("unit_price"))
                .shape[0]
            )

            logging.info(f"Total of {cnt:,} price(s) got discounted.")

    def _insert_record(self, table: str, df: pl.DataFrame) -> None:
        """Insert records into the specified table."""
        data = df.write_json()
        data = json.loads(data)

        for i in range(0, len(data), self.BATCH_INSERT):
            self.supabase_client.table(table) \
                .insert(data[i:i+self.BATCH_INSERT]) \
                .execute()

    def _update_database(self) -> None:
        """Insert data into the database and execute necessary updates."""
        self._insert_record("items", self.df_item)

        self.supabase_client.table("prices") \
            .delete().in_("effective_date", self.date_expiry) \
            .execute()

        self._insert_record("prices", self.df_price)

        self.supabase_client.rpc("update_deals").execute()

        logging.info("Updated the database.")

    def _send_price_alert(self) -> None:
        """Send price alert notification to users via webhook."""
        if datetime.now(self.HKT).isocalendar().weekday not in [5, 6]:  # skip the first 2 days of promotion week
            response = self.supabase_client.rpc("get_users").execute()

            for data in response.data:
                params = {
                    "url": self.API_BOT,
                    "json": {
                        "message": {
                            "from": {
                                "id": data["_id"],
                            },
                            "text": "/alert",
                        }
                    }
                }

                requests.post(**params, timeout=30)

            logging.info(f"Blasted price alert(s) to {len(response.data)} user(s).")
        else:
            logging.info("No price alerts for Friday and Saturday.")

    def _push_error_notification(self, msg: str) -> None:
        """Send error notification to the developer via Telegram webhook."""
        try:
            params = {
                "url": self.API_BOT,
                "json": {
                    "message": {
                        "from": {
                            "id": self.DEVELOPER_ID,
                        },
                        "text": "/error",
                    }
                }
            }

            requests.post(**params, timeout=30)

            logging.error(f"Notified the developer for the broken pipeline: {msg}")
        except Exception as e:
            logging.error(f"Failed to notify the developer the the broken pipeline: (1){msg} (2) {e}")

    def watch(self) -> None:
        """Execute the pipeline."""
        try:
            self._fetch_opw_versions()
            self._download_opw_data()
            self._cleanse_opw_data()
            self._analyse_promotion()
            self._update_database()
            self._send_price_alert()
        except Exception as e:
            self._push_error_notification(e)


if __name__ == "__main__":
    watchdog = SuperPriceWatchdogPipeline()
    watchdog.watch()
