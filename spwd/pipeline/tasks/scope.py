import json
from datetime import timedelta

import luigi
import polars as pl
import requests

from ...shared.clients import SUPABASE_CLIENT
from ...shared.logging import LUIGI_LOGGER
from ...shared.settings import BASE_DIR, Endpoints, Settings


class OpwVersions(luigi.Task):
    """Get available OPW file versions and windowing period."""
    date = luigi.DateParameter()

    def output(self):
        return luigi.LocalTarget(
            BASE_DIR
            / "data"
            / "scope"
            / f"opw_version_{self.date:%Y%m%d}.json"
        )

    def run(self):
        date_version = self._fetch_latest_records()
        existing_dates = self._get_existing_records()
        data = self._update_windows(date_version, existing_dates)

        with self.output().open("w") as f:
            json.dump(data, f)

        LUIGI_LOGGER.info(f"\t- Outstanding date(s): {data.keys()}")

    def _fetch_latest_records(self) -> dict[str, str]:
        end = self.date - timedelta(days=1)
        start = end - timedelta(days=Settings.DELTA - 1)
        dates = start.strftime("%Y%m%d"), end.strftime("%Y%m%d")

        response = requests.get(Endpoints.VERSION.format(*dates), timeout=10)
        response.raise_for_status()

        response_data = response.json()
        versions = response_data.get("timestamps", [])

        return dict(
            pl.DataFrame({"version": versions})
            .with_columns(
                pl.col("version").str.slice(0, 8)
                .str.to_date("%Y%m%d")
                .alias("date")
            )
            .with_columns(
                (pl.col("date") - pl.duration(days=1))  # OPW is 1 day delayed
                .dt.strftime("%Y%m%d")
            )
            .select("date", "version")
            .iter_rows()
        )

    def _get_existing_records(self) -> list[str]:
        response = SUPABASE_CLIENT.rpc("get_dates").execute()
        return [row["date"] for row in response.data]

    def _update_windows(self, date_version, existing_dates) -> dict[str, str]:
        return {
            record_date: version
            for record_date, version in date_version.items()
            if record_date not in existing_dates
        }
