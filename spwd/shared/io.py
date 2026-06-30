import os
import tempfile
from pathlib import Path

import luigi
import polars as pl


def read_target(target: luigi.Target) -> pl.DataFrame:
    with target.open("rb") as source:
        return pl.read_parquet(source)


def write_target(df: pl.DataFrame, target: luigi.Target) -> None:
    final_path = Path(target.path)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        dir=final_path.parent,
        suffix=".tmp",
        prefix=final_path.stem + "_",
    )
    try:
        with os.fdopen(fd, "wb") as destination:
            df.write_parquet(destination)
        os.replace(tmp_path, final_path)
    except Exception:
        os.unlink(tmp_path)
        raise
