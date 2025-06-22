from pathlib import Path

import pytz
import requests
from flask import Flask
from matplotlib.font_manager import FontProperties
from supabase import create_client
from supabase.client import ClientOptions

from .config import Config
from .routes.error import bp as bp_errors
from .routes.index import bp as bp_index
from .routes.pipeline import bp as bp_pipeline
from .routes.response import bp as bp_response
from .routes.repository import bp as bp_repository
from .routes.robots import bp as bp_robots


def create_app():
    app = Flask(__name__)
    app.config.from_object(Config)

    app.font = FontProperties(
        fname=Path(__file__).parent.parent \
            / "config" / "NotoSansCJK-Bold.ttc"
    )

    app.hkt = pytz.timezone(app.config["TIMEZONE"])

    app.supabase_client = create_client(
        app.config["SUPABASE_URL"],
        app.config["SUPABASE_KEY"],
        ClientOptions(schema=app.config["SUPABASE_SCHEMA"]),
    )

    app.register_blueprint(bp_errors)
    app.register_blueprint(bp_index)
    app.register_blueprint(bp_pipeline)
    app.register_blueprint(bp_repository)
    app.register_blueprint(bp_response)
    app.register_blueprint(bp_robots)

    requests.get(app.config["API_WEBHOOK"], timeout=30)

    return app
