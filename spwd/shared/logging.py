import logging


BOT_LOGGER = logging.getLogger("superpricewatchdog")
BOT_LOGGER.handlers.clear()

LUIGI_LOGGER = logging.getLogger("luigi-interface")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,
    )

    for library in ["httpx", "werkzeug"]:
        logging.getLogger(library).setLevel(logging.WARNING)
