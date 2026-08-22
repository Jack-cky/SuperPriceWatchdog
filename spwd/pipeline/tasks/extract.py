import json

import luigi
import polars as pl
import requests

from .scope import OpwVersions
from ...shared.io import write_target
from ...shared.logging import LUIGI_LOGGER
from ...shared.settings import BASE_DIR, Endpoints


class OpwDownloader(luigi.Task):
    """Download OPW data and convert data to DataFrames."""
    date = luigi.DateParameter()

    def requires(self):
        return OpwVersions(date=self.date)

    def output(self):
        return [
            luigi.LocalTarget(
                BASE_DIR
                / "data"
                / "raw"
                / f"items_{self.date:%Y%m%d}.parquet"
            ),
            luigi.LocalTarget(
                BASE_DIR
                / "data"
                / "raw"
                / f"prices_{self.date:%Y%m%d}.parquet"
            ),
        ]

    def run(self):
        with self.input().open("r") as f:
            date_version = json.load(f)["versions"]

        df_item, df_price = self._download_records(date_version)

        if df_item.is_empty() or df_price.is_empty():
            df_empty = pl.DataFrame({"empty": []})
            df_item = df_price = df_empty

        write_target(df_item, self.output()[0])
        write_target(df_price, self.output()[1])

        LUIGI_LOGGER.info(
            f"\t- Total of raw items: {len(df_item):,}\n"
            f"\t- Total of raw prices: {len(df_price):,}"
        )

    def _download_records(self, date_version) -> tuple[pl.DataFrame]:
        prices, items = [], []
        for record_date, version in date_version.items():
            response = requests.get(Endpoints.FILE.format(version), timeout=10)
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
                        "code": code, "date": record_date,
                        **smkt_price.get(smkt, {}),
                        **smkt_offer.get(smkt, {}),
                    }
                    for smkt in set(smkt_price) | set(smkt_offer)
                ]

                prices += price
                items.append(item)

        return pl.json_normalize(items), pl.from_records(prices)
