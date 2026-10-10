from datetime import datetime

from flask import Blueprint, current_app, request

from ..services import send_response, slash_alert

bp = Blueprint("alert", __name__)


@bp.route("/alert", methods=["POST"])
def blast_alerts() -> str:
    token = request.headers.get("X-Internal-Token")
    if token != current_app.config["API_SECRET"]:
        return "", 403

    response = current_app.supabase_client.rpc("get_users").execute()

    if datetime.now(current_app.hkt).isoweekday() not in [5, 6]:  # skip the first 2 days of promotion week
        for row in response.data:
            msg = slash_alert(row["id"])
            send_response(
                row["id"],
                msg,
                None,
                {
                    "inline_keyboard": [[
                        {
                            "text": "💤 Snooze",
                            "callback_data": "/snooze",
                        },
                    ]],
                },
            )

    current_app.logger.info(
        f"[Daily Alert] Blasted to {len(response.data):,} users."
    )

    return "", 200
