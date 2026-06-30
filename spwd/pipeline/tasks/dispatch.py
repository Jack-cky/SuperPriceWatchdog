import luigi

from .load import OpwDatabase
from ...shared.settings import BASE_DIR


class EntryPoint(luigi.Task):
    """Mark completion of the daily pipeline execution."""
    date = luigi.DateParameter()

    def requires(self):
        return OpwDatabase(date=self.date)

    def output(self):
        return luigi.LocalTarget(
            BASE_DIR
            / "logs"
            / f"daily_{self.date:%Y%m%d}.txt"
        )

    def run(self):
        with self.output().open("w") as f:
            f.write(f"Completed daily execution on {self.date:%Y%m%d}.")
