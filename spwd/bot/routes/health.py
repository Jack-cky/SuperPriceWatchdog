from flask import Blueprint

bp = Blueprint("health", __name__)


@bp.route("/health", methods=["GET"])
def health_check() -> str:
    return "OK", 200
