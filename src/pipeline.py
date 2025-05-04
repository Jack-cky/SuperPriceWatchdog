"""
SuperPriceWatchdog Pipeline for monitoring price changes using Open Price Watch
(OPW) data. This pipeline implements an ETLT (Extract, Transform, Load,
Transform) process to manage price data and alert users of any price changes.
The pipeline is designed to run daily, downloading price data, cleansing it,
and updating a database with new prices and items.
"""
import configparser
import json
import logging
import os
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import luigi
import polars as pl
import pytz
import requests
from dotenv import load_dotenv
from supabase import create_client
from supabase.client import ClientOptions


PTH = Path(os.path.dirname(__file__).split(os.sep)[-2])
PTH = [PTH, os.sep/PTH][not os.path.isdir(PTH / "config")]

load_dotenv(PTH / "config" / ".env")

CONFIG = configparser.ConfigParser()
CONFIG.read(PTH / "config" / "config.ini")

for config in [
    ("worker", "keep-alive", "True"),
    ("scheduler", "retry_delay", CONFIG.get("SCHEDULER", "RETRY_DELAY")),
    ("scheduler", "retry_count", CONFIG.get("SCHEDULER", "RETRY_COUNT")),
]:
    luigi.configuration.get_config().set(*config)

LOGGER = logging.getLogger("luigi-interface")


class OpwVersions(luigi.Task):
    """Get available OPW file versions and windowing period."""
    api = CONFIG.get("API", "VERSION")

    today = datetime.now(pytz.timezone(CONFIG.get("TIME", "TIMEZONE")))
    batch = CONFIG.getint("TASK", "BATCH")
    delta = CONFIG.getint("TASK", "DELTA")

    supabase_client = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY"),
        ClientOptions(schema=CONFIG.get("DATABASE", "SCHEMA")),
    )

    def output(self):
        return luigi.LocalTarget(PTH / "data" / "opw_version.json")

    def run(self):
        date_version = self._fetch_latest_records()

        existing_dates = self._get_existing_records()

        data = self._update_windows(date_version, existing_dates)

        with self.output().open("w") as f:
            json.dump(data, f)

        LOGGER.info(
            f"\t- Outstanding date(s): {data['version']}\n"
            f"\t- Expiring date(s): {data['expiry']}"
        )

    def _fetch_latest_records(self) -> dict:
        versions = set()
        for delta in range(0, self.delta, self.batch):
            end = self.today - timedelta(days=1+delta)
            start = end - timedelta(days=self.batch-1)
            dates = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

            response = requests.get(self.api.format(*dates), timeout=20)
            response.raise_for_status()

            date = response.json()
            versions.update(date.get("timestamps", []))

        return dict(
            pl.DataFrame({"version": list(versions)})
            .with_columns(
                pl.col("version").str.slice(0, 8)
                    .str.to_date("%Y%m%d")
                    .alias("date")
            )
            .with_columns(
                (pl.col("date")-pl.duration(days=1))  # OPW is one day delayed
                    .dt.strftime("%Y%m%d")
            )
            .select("date", "version")
            .iter_rows()
        )

    def _get_existing_records(self) -> list:
        response = self.supabase_client.rpc("get_dates").execute()

        return [data["_date"] for data in response.data]

    def _update_windows(
            self,
            date_version,
            existing_dates,
    ) -> dict[str, dict[str, str]|list]:
        date_expiry = []
        for date in existing_dates:
            if date in date_version:
                date_version.pop(date, None)
            else:
                date_expiry.append(date)

        return {"version": date_version, "expiry": date_expiry}


class OpwDownloader(luigi.Task):
    """Download OPW data and convert data to DataFrames."""
    api = CONFIG.get("API", "FILE")

    def requires(self):
        return OpwVersions()

    def output(self):
        return [
            luigi.LocalTarget(PTH / "data" / "raw_items.parquet"),
            luigi.LocalTarget(PTH / "data" / "raw_prices.parquet"),
        ]

    def run(self):
        with self.input().open("r") as f:
            date_version = json.load(f)["version"]

        df_item, df_price = self._download_records(date_version)

        if df_item.is_empty() or df_price.is_empty():
            df_empty = pl.DataFrame({"empty": []})
            df_item = df_price = df_empty

        df_item.write_parquet(self.output()[0].path)
        df_price.write_parquet(self.output()[1].path)

        LOGGER.info(
            f"\t- Total of raw items: {len(df_item):,}\n"
            f"\t- Total of raw prices: {len(df_price):,}"
        )

    def _download_records(
            self,
            date_version,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        prices, items = [], []
        for date, version in date_version.items():
            response = requests.get(self.api.format(version), timeout=20)
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

        return pl.json_normalize(items), pl.from_records(prices)


class OpwCleanser(luigi.Task):
    """Cleanse OPW data to match the defined schema in the database."""
    supabase_client = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY"),
        ClientOptions(schema=CONFIG.get("DATABASE", "SCHEMA")),
    )

    def requires(self):
        return OpwDownloader()

    def output(self):
        return [
            luigi.LocalTarget(PTH / "data" / "cleansed_items.parquet"),
            luigi.LocalTarget(PTH / "data" / "cleansed_prices.parquet"),
        ]

    def run(self) -> None:
        df_item = pl.read_parquet(self.input()[0].path)
        df_price = pl.read_parquet(self.input()[1].path)

        df_item = self._cleanse_item_data(df_item)
        df_price = self._cleanse_price_data(df_price)

        df_item.write_parquet(self.output()[0].path)
        df_price.write_parquet(self.output()[1].path)

        LOGGER.info(
            f"\t- Total of new items: {len(df_item):,}\n"
            f"\t- Total of new prices: {len(df_price):,}"
        )

    def _cleanse_item_data(self, df_item: pl.DataFrame) -> pl.DataFrame:
        if not df_item.is_empty():
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

            df_item = (
                df_item
                .unique(subset="code")  # keep unique SKUs
                .filter(~pl.col("code").is_in(sku_list))  # filter out existing SKUs
                .select(cols)
                .rename(cols)
            )

        return df_item

    def _cleanse_price_data(self, df_price: pl.DataFrame) -> pl.DataFrame:
        if not df_price.is_empty():
            cols = {
                "code": "sku",
                "date": "effective_date",
                "supermarketCode": "supermarket",
                "en": "promotion_en",
                "zh-Hant": "promotion_zh",
                "price": "original_price",
            }

            df_price = (
                df_price
                .with_columns(
                    pl.col(["en", "zh-Hant"]).fill_null("No Promotion"),  # default no promotion
                    pl.col("price").str.extract(r"([\d\.]+)")
                        .cast(pl.Float32)
                        .fill_null(0),  # ensure a valid price for each record
                )
                .select(cols)
                .rename(cols)
            )

        return df_price


class OpwAnalyser(luigi.Task):
    """Categorise promotions and calculate unit prices."""
    threshold = CONFIG.getfloat("TASK", "THRESHOLD")

    def requires(self):
        return OpwCleanser()

    def output(self):
        return luigi.LocalTarget(PTH / "data" / "analysed_prices.parquet")

    def run(self) -> None:
        df_price = pl.read_parquet(self.input()[1].path)

        df_price, cnt = self._calculate_promotion_prices(df_price)

        df_price.write_parquet(self.output().path)

        LOGGER.info(f"\t- Total of discount prices: {cnt:,}")

    def _split_components(self, mkt_txt: str) -> list[str]:
        txt = re.sub(r"\s?wk\d+\s?", "", mkt_txt.lower().strip())
        txt = re.sub(r"; |/|[a-z]\.[a-z]", "<sep>", txt)
        txt = re.sub(r"half price", "50%", txt)
        txt = re.sub(r"second", "2nd", txt)

        return list(map(str.strip, txt.split("<sep>")))

    def _map_pattern_tags(self, promotion: str) -> str:
        pattern_tags = {
            r"\$\d+(\.\d+)?": "<AMT>",
            r"\d+(\.\d+)?\%": "<PCT>",
            r"\d+": "<CNT>",
        }

        txt = promotion.lower().strip()

        for pat, tag in pattern_tags.items():
            txt = re.sub(pat, tag, txt)

        return txt

    def _extract_tag_values(self, data: dict[str, str]) -> list[float]:
        values = []
        if data["category"]:
            promotions = data["promotion"].split()
            patterns = data["pattern"].split()
            for promotion, pattern in zip(promotions, patterns):
                if re.search(r"<AMT>|<CNT>|<PCT>", pattern):
                    nums = re.findall(r"(\d+\.?\d{,2})", promotion)
                    values += [float(num) for num in nums]

        return values

    def _calculate_discount_price(
            self,
            data: dict[str, float | list[float]],
    ) -> float:
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
        except:  # ignore promotions that do not apply to any of the rules
            pass

        return discount if price * self.threshold < discount else price

    def _calculate_promotion_prices(
        self,
        df_price: pl.DataFrame,
        cnt: int=0,
    ) -> pl.DataFrame:
        if not df_price.is_empty():
            df_price = (
                df_price
                .with_columns(
                    pl.col("promotion_en")
                        .map_elements(
                            self._split_components,
                            return_dtype=list[str],
                        )
                        .alias("promotion"),  # split description with multiple promotions
                )
                .explode("promotion")  # expand row per promotion
                .with_columns(
                    pl.col("promotion")
                        .map_elements(self._map_pattern_tags, return_dtype=str)
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
                        .map_elements(
                            self._extract_tag_values,
                            return_dtype=list[float],
                        )
                        .alias("value"),  # get numeric values according to pattern
                )
                .with_columns(
                    pl.struct("original_price", "pattern", "category", "value")
                        .map_elements(
                            self._calculate_discount_price,
                            return_dtype=float,
                        )
                        .alias("unit_price"),  # calculate price based on defined rules
                )
                .sort(["sku", "effective_date", "supermarket", "unit_price"])
                .unique(subset=["sku", "effective_date", "supermarket"])
                .drop([
                    "amt", "cnt", "pct",
                    "promotion", "pattern", "category", "value",
                ])
            )

            cnt += (
                df_price
                .filter(pl.col("original_price")!=pl.col("unit_price"))
                .shape[0]
            )

        return df_price, cnt


class DatabaseRecords(luigi.Task):
    """Insert data into the database and execute necessary updates."""
    supabase_client = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY"),
        ClientOptions(schema=CONFIG.get("DATABASE", "SCHEMA")),
    )

    def requires(self):
        return [
            OpwVersions(),
            OpwCleanser(),
            OpwAnalyser(),
        ]

    def output(self):
        return luigi.LocalTarget(PTH / "logs" / "task_database.txt")

    def run(self):
        with self.input()[0].open("r") as f:
            data = json.load(f)

        df_item = pl.read_parquet(self.input()[1][0].path)
        df_price = pl.read_parquet(self.input()[2].path)

        if data["version"]:
            self._update_items(df_item)
            self._update_prices(df_price, data["expiry"])
            self._update_deals()
        else:
            self._log_omission()

        with self.output().open("w") as f:
            f.write("Completed updating database.")

        LOGGER.info("\t- Updated the database.")

    def _insert_record(
            self,
            table: str,
            df: pl.DataFrame,
            batch: int=10_000,
    ) -> None:
        data = json.loads(df.write_json())

        for i in range(0, len(data), batch):
            self.supabase_client.table(table).insert(data[i:i+batch]).execute()

    def _update_items(self, df_item) -> None:
        self._insert_record("items", df_item)

    def _update_prices(self, df_price, date_expiry) -> None:
        self.supabase_client.table("prices") \
            .delete().in_("effective_date", date_expiry) \
            .execute()

        self._insert_record("prices", df_price)

    def _update_deals(self) -> None:
        self.supabase_client.rpc("update_deals").execute()

    def _log_omission(self) -> None:
        self.supabase_client.rpc("log_omission").execute()


class DailyPriceAlert(luigi.Task):
    """Send price alert notification to users via webhook."""
    api = CONFIG.get("API", "BOT").format(os.getenv("FORWARDING_URL"))

    _now = datetime.now(pytz.timezone(CONFIG.get("TIME", "TIMEZONE")))
    latest = (_now - timedelta(days=2)).strftime("%Y%m%d")
    wkday = _now.isocalendar().weekday

    _target = _now.replace(
        hour=CONFIG.getint("TIME", "HOUR"),
        minute=0,
        second=0,
    )
    wait = max(0, (_target-_now).total_seconds())

    supabase_client = create_client(
        os.getenv("SUPABASE_URL"),
        os.getenv("SUPABASE_KEY"),
        ClientOptions(schema=CONFIG.get("DATABASE", "SCHEMA")),
    )

    def requires(self):
        return [
            OpwVersions(),
            DatabaseRecords(),
        ]

    def output(self):
        return luigi.LocalTarget(PTH / "logs" / "task_alert.txt")

    def run(self):
        with self.input()[0].open("r") as f:
            data = json.load(f)

        n_users = 0
        if self.latest in data["version"]:  # ensure `deal` is up to date
            if self.wkday not in [5, 6]:  # skip the first 2 days of promotion week
                time.sleep(self.wait)  # only send alerts at a specific time
                n_users += self._blast_alerts()

        with self.output().open("w") as f:
            f.write("Completed blasting price alert to users.")

        LOGGER.info(f"\t- Blasted price alert to {n_users} users.")

    def _blast_alerts(self) -> int:
        response = self.supabase_client.rpc("get_users").execute()

        for data in response.data:
            requests.post(
                self.api,
                json={
                    "message": {
                        "from": {
                            "id": data["_id"],
                        },
                        "text": "/alert",
                    }
                },
                timeout=30,
            )

        return len(response.data)


class EntryPoint(luigi.Task):
    """Manage task output caches for daily execution."""
    _today = datetime.now(pytz.timezone(CONFIG.get("TIME", "TIMEZONE")))
    tdy = _today.strftime("%Y%m%d")
    ytd = (_today - timedelta(days=1)).strftime("%Y%m%d")

    def requires(self):
        return DailyPriceAlert()

    def output(self):
        return luigi.LocalTarget(PTH / "logs" / f"task_{self.tdy}.txt")

    def run(self):
        self._clear_task_caches()

        with self.output().open("w") as f:
            f.write(f"Completed daily execution on {self.tdy}.")

    def _get_task_outputs(self, task):
        outputs = task.output()
        if isinstance(outputs, luigi.LocalTarget):
            outputs = [outputs]

        file_pths = [output.path for output in outputs]

        task_prev = task.requires()
        if task_prev:
            if isinstance(task_prev, luigi.Task):
                task_prev = [task_prev]

            for _task in task_prev:
                file_pths += self._get_task_outputs(_task)

        return file_pths

    def _clear_task_caches(self):
        file_pths = self._get_task_outputs(self.requires())
        file_pths.append(PTH / "logs" / f"task_{self.ytd}.txt")

        for file_pth in file_pths:
            if os.path.exists(file_pth):
                os.remove(file_pth)


if __name__ == "__main__":
    luigi.build([EntryPoint()], local_scheduler=True)
