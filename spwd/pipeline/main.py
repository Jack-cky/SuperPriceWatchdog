from datetime import datetime
from pathlib import Path

import luigi

from .tasks import EntryPoint
from ..shared.settings import BASE_DIR, Settings


def configure_luigi_worker() -> None:
    for config in [
        ("worker", "keep-alive", "True"),
        ("worker", "no_install_shutdown_handler", "True"),
        ("scheduler", "retry_delay", Settings.RETRY_DELAY),
        ("scheduler", "retry_count", Settings.RETRY_COUNT),
    ]:
        luigi.configuration.get_config().set(*config)


def setup_output_folders() -> None:
    for path in [
        BASE_DIR / "data" / "interim",
        BASE_DIR / "data" / "processed",
        BASE_DIR / "data" / "raw",
        BASE_DIR / "data" / "scope",
        BASE_DIR / "logs",
    ]:
        Path(path).mkdir(parents=True, exist_ok=True)


def main(workers: int = 1, local_scheduler: bool = True) -> None:
    configure_luigi_worker()
    setup_output_folders()

    luigi.build(
        [EntryPoint(date=datetime.now(Settings.HKT))],
        workers=workers,
        local_scheduler=local_scheduler,
    )


if __name__ == "__main__":
    main()
