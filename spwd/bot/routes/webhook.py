import requests
from flask import Blueprint, current_app

bp = Blueprint("webhook", __name__)


@bp.route("/webhook", methods=["GET"])
def setup_webhook() -> str:
    try:
        response = requests.get(current_app.config["WEBHOOK"], timeout=10)
        response.raise_for_status()

        current_app.logger.info("Webhook setup successful.")
    except requests.RequestException as e:
        current_app.logger.error(f"Failed to set up webhook: {e}")

        return "", 500

    return "", 200
