from flask import Blueprint, send_from_directory, current_app


bp = Blueprint("robots", __name__)


@bp.route("/robots.txt")
def get_crawler_directives() -> str:
    return send_from_directory(current_app.static_folder, "robots.txt")
