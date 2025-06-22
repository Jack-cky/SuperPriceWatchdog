from flask import Blueprint, render_template


bp = Blueprint("error", __name__)


@bp.app_errorhandler(404)
def not_found(error):
    return render_template("404.html"), 404


@bp.app_errorhandler(500)
def internal_server_error(error):
    return render_template("500.html"), 500
