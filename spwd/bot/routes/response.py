import re

from flask import Blueprint, current_app, request

from ..models import ExternalSlash, InternalSlash, HiddenSlash
from ..services import (
    answer_callback,
    send_response,
    slash_alert,
    slash_bye,
    slash_edit,
    slash_error,
    slash_help,
    slash_lang,
    slash_list,
    slash_lucky,
    slash_plot,
    slash_snooze,
    slash_start,
    slash_sub,
    slash_unk,
)

bp = Blueprint("response", __name__)


def get_command(usr_msg: str) -> tuple[str, str]:
    if usr_input := re.findall(r"/opw/product/(P?\d+)", usr_msg):
        return "/edit", usr_input[0]

    if parts := usr_msg.split():
        slash = parts[0]
        if re.fullmatch(r"/P?\d+", slash):
            return "/plot", slash[1:]

        valid_slashes = (
            [s.value for s in ExternalSlash]
            + [s.value for s in HiddenSlash]
        )
        if slash in valid_slashes:
            return slash, ""

    return "/other", ""


@bp.route("/reply", methods=["POST"])
def handle_message() -> str:
    data = request.get_json()
    if query := data.get("callback_query"):
        answer_callback(query["id"])
        data = {
            "message": {
                "from": query["from"],
                "text": query["data"],
                "date": query.get("message", {}).get("date"),  # when the alert was sent
            },
        }

    if "message" in data:
        usr_id = data["message"]["from"]["id"]
        usr_msg = data["message"].get("text", "non-text message")

        msg, img = "", None
        try:
            slash, code = get_command(usr_msg)
            match slash:
                # case InternalSlash.ALERT:
                #     msg = slash_alert(usr_id)
                case InternalSlash.EDIT:
                    msg = slash_edit(usr_id, code)
                # case InternalSlash.ERROR:
                #     msg = slash_error("pipeline")
                case InternalSlash.PLOT:
                    img = slash_plot(usr_id, code)
                case ExternalSlash.HELP:
                    msg = slash_help(usr_id)
                case ExternalSlash.LANG:
                    msg = slash_lang(usr_id)
                case ExternalSlash.LIST:
                    msg = slash_list(usr_id)
                case ExternalSlash.LUCKY:
                    msg = slash_lucky(usr_id)
                case ExternalSlash.SNOOZE:
                    msg = slash_snooze(usr_id, data["message"]["date"])
                case ExternalSlash.START:
                    msg = slash_start(
                        usr_id,
                        data["message"]["from"]["first_name"],
                        data["message"]["from"]["language_code"],
                    )
                case ExternalSlash.SUB:
                    msg = slash_sub(usr_id)
                case HiddenSlash.BYE:
                    msg = slash_bye(usr_id)
                case _:
                    msg = slash_unk("unk")
        except Exception:
            msg = slash_error("user")
            current_app.logger.error(
                f"Failed to parse {usr_id} command:",
                exc_info=True,
            )
        finally:
            send_response(usr_id, msg, img)
            current_app.logger.info(f"Message ({usr_id}): {usr_msg}")
    else:
        current_app.logger.warning("No message found in the request.")

        return "", 204

    return "", 200
