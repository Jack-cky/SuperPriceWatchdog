from flask import Flask
from matplotlib.font_manager import FontProperties

from .routes import (
    bp_alert,
    bp_errors,
    bp_health,
    bp_index,
    bp_response,
    bp_robots,
    bp_webhook,
)
from ..shared.clients import SUPABASE_CLIENT
from ..shared.logging import BOT_LOGGER, configure_logging
from ..shared.settings import BASE_DIR, Endpoints, Settings


def create_app():
    app = Flask(__name__)

    for config in [Endpoints, Settings]:
        app.config.from_object(config)

    for prefix, bps in {
        "": [bp_errors, bp_health, bp_index, bp_robots],
        "/api/v1": [bp_alert, bp_response, bp_webhook],
    }.items():
        for bp in bps:
            app.register_blueprint(bp, url_prefix=prefix)

    app.font = FontProperties(
        fname=BASE_DIR / "config" / "NotoSansCJK-Bold.ttc"
    )
    app.hkt = Settings.HKT
    app.logger = BOT_LOGGER
    app.supabase_client = SUPABASE_CLIENT

    return app


def main() -> None:
    configure_logging()

    app = create_app()
    app.run(host="0.0.0.0", port=5000)


if __name__ == "__main__":
    main()
