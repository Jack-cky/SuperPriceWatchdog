import json

import luigi
import polars as pl

from .analyse import OpwAnalyser
from .cleanse import OpwCleanser
from ...shared.clients import SUPABASE_CLIENT
from ...shared.io import read_target
from ...shared.logging import LUIGI_LOGGER
from ...shared.settings import BASE_DIR, Settings


class OpwDatabase(luigi.Task):
    """Update data in the database and execute necessary updates."""
    date = luigi.DateParameter()

    def requires(self):
        return {
            "cleanser": OpwCleanser(date=self.date),
            "analyser": OpwAnalyser(date=self.date),
        }

    def output(self):
        return luigi.LocalTarget(
            BASE_DIR
            / "logs"
            / f"database_{self.date:%Y%m%d}.txt"
        )

    def run(self):
        df_item = read_target(self.input()["cleanser"][0])
        df_price = read_target(self.input()["analyser"])

        if df_price.is_empty():
            self._log_omission()
        else:
            self._update_items(df_item)
            self._update_supermarkets(df_price)
            self._update_prices(df_price)
            self._update_deals()

        with self.output().open("w") as f:
            f.write("Completed updating database.")

        LUIGI_LOGGER.info("\t- Updated the database.")

    def _upsert_record(
        self,
        table: str,
        df: pl.DataFrame,
        batch: int = 1_000,
    ) -> None:
        data = json.loads(df.write_json())
        total = len(data)
        for i in range(0, total, batch):
            LUIGI_LOGGER.info(
                f"[DB] {table}: upserting "
                f"{i+1}-{min(i+batch, total)} / {total}"
            )
            SUPABASE_CLIENT.table(table).upsert(data[i:i+batch]).execute()

    def _update_items(self, df_item: pl.DataFrame) -> None:
        response = SUPABASE_CLIENT.rpc("get_skus").execute()
        existing_skus = [row["sku"] for row in response.data]

        df_item = df_item.filter(~pl.col("sku").is_in(existing_skus))
        self._upsert_record("items", df_item)

    def _update_supermarkets(self, df_price: pl.DataFrame) -> None:
        response = SUPABASE_CLIENT.rpc("get_supermarkets").execute()
        existing = {row["supermarket"] for row in response.data}

        df_smkt = (
            df_price
            .select("supermarket")
            .drop_nulls()
            .unique()
            .filter(~pl.col("supermarket").is_in(existing))
        )

        self._upsert_record("supermarkets", df_smkt)

    def _update_prices(self, df_price: pl.DataFrame) -> None:
        self._upsert_record("prices", df_price)
        SUPABASE_CLIENT.rpc(
            "remove_legacy_prices",
            {"p_interval": Settings.DELTA},
        ).execute()

    def _update_deals(self) -> None:
        SUPABASE_CLIENT.rpc("update_deals").execute()

    def _log_omission(self) -> None:
        SUPABASE_CLIENT.rpc("log_omission").execute()
