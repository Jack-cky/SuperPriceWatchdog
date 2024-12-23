import json
import logging
import os
import re
from datetime import datetime, timedelta

import polars as pl
import requests
from dotenv import load_dotenv
from supabase import create_client, Client
from supabase.client import ClientOptions


load_dotenv("./SuperPriceWatchdog/config/.env")

logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.WARNING,
    format="[%(levelname)s] %(asctime)s %(message)s",
)


def log(msg: str) -> None:
    """Replace default INFO logging."""
    print(f"[INFO] {datetime.now()} {msg}")


class SuperPriceWatchdogPipeline:
    """
    A pipeline for monitoring price changes using Open Price Watch (OPW) data.
    This class performs ETL (Extract, Transform, Load) operations on price data
    and alerts users of price changes.
    
    The pipeline is designed to run daily, downloading price data, cleansing it,
    and updating a database with new prices and items.
    """
    
    def __init__(self):
        self.api = self._initialise_api()
        self.supabase_client = self._initialise_database_client()
        self.date_range = self._initialise_date_range()
        self.date_expiry = []
        self.date_version = {}
    
    def _initialise_api(self) -> dict[str, str]:
        """Define API URLs for OPW data and Telegram webhook."""
        api_base = "https://api.data.gov.hk/v1/historical-archive"
        source_url = (
            "https://online-price-watch.consumer.org.hk"
            "/opw/opendata/pricewatch.json"
        )
        
        version = (
            f"{api_base}/list-file-versions"
            f"?url={source_url}&start={{}}&end={{}}"
        )
        file = f"{api_base}/get-file?url={source_url}&time={{}}"
        
        webhook_url = os.getenv("FORWARDING_URL")
        bot = f"{webhook_url}/api/v1/reply"
        
        return {"version": version, "file": file, "bot": bot}
    
    def _initialise_database_client(self) -> Client:
        """Define connection to Supabase database."""
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        schema = os.getenv("SUPABASE_SCHEMA")
        
        client = create_client(url, key, ClientOptions(schema=schema))
        
        response = client.rpc("check_connection").execute()
        if not response.data[0].get("okay"):
            raise Exception("Failed to connect to the database.")
        
        return client
    
    def _initialise_date_range(self) -> tuple[tuple[str]]:
        """Define date range for detching OPW data"""
        delta = int(os.getenv("DELTA"))
        schedule = os.getenv("SCHEDULE")
        
        dt_end = datetime.today() - timedelta(days=1)
        dt_start = dt_end - timedelta(days=delta-1)
        
        start = dt_start.strftime("%Y%m%d")
        end = dt_end.strftime("%Y%m%d")
        
        dt_backtrack = datetime.today() - timedelta(days=2)
        self.backtrack = dt_backtrack.strftime("%Y%m%d") + f"-{schedule}"
        
        return (start, end), (end, end)
    
    def _fetch_opw_versions(self) -> None:
        """Get the available OPW versions and update date windowing."""
        for start_end in self.date_range:
            response = requests.get(self.api["version"].format(*start_end))
            response.raise_for_status()
            
            data = response.json()
            dates = data.get("data-dictionary-dates", [])
            versions = data.get("timestamps", [])
            
            self.date_version.update(dict(zip(dates, versions)))
        
        response = self.supabase_client.rpc("get_dates").execute()
        
        for data in response.data:
            if data["_date"] in self.date_version and self.date_version[data["_date"]] < self.backtrack:
                self.date_version.pop(data["_date"], None)
            else:
                self.date_expiry.append(data["_date"])
        
        log(f"Window of {len(self.date_version)} day(s) to be updated.")
    
    def _download_opw_data(self) -> None:
        """Get pending OPW data and process it into DataFrames."""
        prices, items = [], []
        
        for date, version in self.date_version.items():
            response = requests.get(self.api["file"].format(version))
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
        
        log(
            f"Total of {len(items):,} raw item(s) and "
            f"{len(prices):,} raw price(s)."
        )
    
    def _cleanse_opw_data(self) -> None:
        """Cleanse up the OPW data based on the defined schema on database."""
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
        
        log(
            f"Total of {len(self.df_item):,} new item(s) and "
            f"{len(self.df_price):,} new price(s)."
        )
    
    def _analyse_promotion(self) -> None:
        """Categorise promotions and calculate unit prices."""
        def split_promotion(mkt_txt: str) -> list[str]:
            txt = re.sub(r"\s?wk\d+\s?", "", mkt_txt.lower().strip())
            txt = re.sub(r"; |/|[a-z]\.[a-z]", "<sep>", txt)
            txt = re.sub(r"half price", "50%", txt)
            txt = re.sub(r"second", "2nd", txt)
            
            return list(map(str.strip, txt.split("<sep>")))
        
        def get_pattern(promotion: str) -> str:
            pattern_tags = {
                r"\$\d+(\.\d+)?": "<AMT>",
                r"\d+(\.\d+)?\%": "<PCT>",
                r"\d+": "<CNT>",
            }
            
            txt = promotion.lower().strip()
            
            for pat, tag in pattern_tags.items():
                txt = re.sub(pat, tag, txt)
            
            return txt
        
        def extract_values(data: dict[str, str]) -> list[float]:
            values = []
            if data["category"]:
                promotions = data["promotion"].split()
                patterns = data["pattern"].split()
                for promotion, pattern in zip(promotions, patterns):
                    if re.search(r"<AMT>|<CNT>|<PCT>", pattern):
                        nums = re.findall(r"(\d+\.?\d{,2})", promotion)
                        values += [float(num) for num in nums]
            
            return values
        
        def calculate_discount(data: dict[str, float | list[float]]) -> float:
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
                        if re.search("<CNT>\s.*save" , pattern):
                            discount = price * values[0] - values[1]
                            discount /= values[0]
                        elif re.search("<CNT>\s" , pattern):
                            discount = values[1] / values[0]
                    case 5:
                        if re.search("free the most expensive one" , pattern):
                            discount = price * (values[0] - 1)
                            discount /= values[0]
                        elif re.search("get <CNT> free" , pattern):
                            discount = price * values[0]
                            discount /= values[0] + values[1]
                    case 6:
                        if re.search("<CNT>\w" , pattern):
                            discount = price * (values[0] - 1) \
                                + price * (1 - values[1] / 100)
                            discount /= values[0]
                        else:
                            discount = price * values[0] * (1 - values[1] / 100)
                            discount /= values[0]
                    case 8:
                        if re.search("<CNT>\w" , pattern):
                            discount = price * (1 - values[0] / 100) \
                                + price * (values[1] - 1)
                            discount /= values[1]
                        else:
                            discount = price * (1 - values[0] / 100)
                    case _:
                        discount = price
            except Exception:
                pass
            
            return discount if price * .3 < discount else price
        
        if not self.df_price.is_empty():
            self.df_price = (
                self.df_price
                .with_columns(
                    pl.col("promotion_en")
                        .map_elements(split_promotion, return_dtype=list[str])
                        .alias("promotion"),  # split description with multiple promotions
                )
                .explode("promotion")  # expand row per promotion
                .with_columns(
                    pl.col("promotion")
                        .map_elements(get_pattern, return_dtype=str)
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
                        .map_elements(extract_values, return_dtype=list[float])
                        .alias("value"),  # get numeric values according to pattern
                )
                .with_columns(
                    pl.struct("original_price", "pattern", "category", "value")
                        .map_elements(calculate_discount, return_dtype=float)
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
            
            log(f"Total of {cnt:,} price(s) got discounted.")
    
    def _update_database(self) -> None:
        """Insert OPW data to the database and execute ETL job."""
        def insert_record(
            table: str, df: pl.DataFrame, batch_size: int=10_000,
        ) -> None:
            data = df.write_json()
            data = json.loads(data)
            
            for i in range(0, len(data), batch_size):
                self.supabase_client.table(table) \
                    .insert(data[i:i+batch_size]) \
                    .execute()
        
        insert_record("items", self.df_item)
        
        self.supabase_client.table("prices") \
            .delete().in_("effective_date", self.date_expiry) \
            .execute()
        
        insert_record("prices", self.df_price)
        
        # self.supabase_client.rpc("remove_prices", {"delta": self.delta}).execute()
        
        self.supabase_client.rpc("update_deals").execute()
        
        log("Updated the database.")
    
    def _send_price_alert(self) -> None:
        """Post alert requests to Telegram webhook."""
        if datetime.today().weekday() != 4:  # skip the first day of promotion week
            response = self.supabase_client.rpc("get_users").execute()
            
            for data in response.data:
                data = {
                    "message": {
                        "from": {
                            "id": data["_id"],
                        },
                        "text": "/alert",
                    }
                }
                
                requests.post(self.api["bot"], json=data)
            
            log(f"Blasted price alert(s) to {len(response.data)} user(s).")
        else:
            log("No price alerts for Friday.")
    
    def watch(self) -> None:
        self._fetch_opw_versions()
        self._download_opw_data()
        self._cleanse_opw_data()
        self._analyse_promotion()
        self._update_database()
        self._send_price_alert()


if __name__ == "__main__":
    watchdog = SuperPriceWatchdogPipeline()
    watchdog.watch()
