import luigi
import polars as pl

from .extract import OpwDownloader
from ...shared.io import read_target, write_target
from ...shared.logging import LUIGI_LOGGER
from ...shared.settings import BASE_DIR


class OpwCleanser(luigi.Task):
    """Cleanse OPW data to match the defined schema in the database."""
    date = luigi.DateParameter()

    def requires(self):
        return OpwDownloader(date=self.date)

    def output(self):
        return [
            luigi.LocalTarget(
                BASE_DIR
                / "data"
                / "processed"
                / f"items_{self.date:%Y%m%d}.parquet"
            ),
            luigi.LocalTarget(
                BASE_DIR
                / "data"
                / "interim"
                / f"prices_{self.date:%Y%m%d}.parquet"
            ),
        ]

    def run(self) -> None:
        df_item = read_target(self.input()[0])
        df_price = read_target(self.input()[1])

        df_item = self._cleanse_item_data(df_item)
        df_price = self._cleanse_price_data(df_price)

        write_target(df_item, self.output()[0])
        write_target(df_price, self.output()[1])

        LUIGI_LOGGER.info(
            f"\t- Total of items: {len(df_item):,}\n"
            f"\t- Total of prices: {len(df_price):,}"
        )

    def _cleanse_item_data(self, df_item: pl.DataFrame) -> pl.DataFrame:
        if df_item.is_empty():
            return df_item

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

        return (
            df_item
            .unique(subset="code")  # keep unique SKUs
            .select([pl.col(old).alias(new) for old, new in cols.items()])
        )

    def _cleanse_price_data(self, df_price: pl.DataFrame) -> pl.DataFrame:
        if df_price.is_empty():
            return df_price

        cols = {
            "code": "sku",
            "date": "effective_date",
            "supermarketCode": "supermarket",
            "en": "promotion_en",
            "zh-Hant": "promotion_zh",
            "price": "original_price",
        }

        return (
            df_price
            .with_columns(
                pl.col("supermarketCode")
                .str.strip_chars()
                .str.to_uppercase()
            )
            .with_columns(
                pl.col(["en", "zh-Hant"]).fill_null("No Promotion"),  # default no promotion
                pl.col("price").str.extract(r"([\d\.]+)")
                .cast(pl.Float32)
                .fill_null(0),  # ensure a valid price for each record
            )
            .select([pl.col(old).alias(new) for old, new in cols.items()])
        )
