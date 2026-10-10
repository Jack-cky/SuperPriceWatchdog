import json

import requests
from flask import current_app


def answer_callback(query_id: str) -> None:
    try:
        requests.post(
            current_app.config["CALLBACK"],
            data={"callback_query_id": query_id},
            timeout=10,
        )
    except requests.RequestException:
        current_app.logger.error(
            f"Failed to answer callback {query_id}:",
            exc_info=True,
        )


def send_response(
    usr_id: int, msg: str, img: bytes | None, markup: dict | None = None
) -> None:
    try:
        if img:
            params = {
                "url": current_app.config["IMG"],
                "data": {
                    "chat_id": usr_id,
                },
                "files": {
                    "photo": img,
                },
            }
        else:
            params = {
                "url": current_app.config["MSG"],
                "data": {
                    "chat_id": usr_id,
                    "text": msg,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": 1,
                    "reply_markup": json.dumps(markup) if markup else None,
                },
            }

        requests.post(**params, timeout=10)
    except requests.RequestException:
        current_app.logger.error(
            f"Failed to reply {usr_id}'s message:",
            exc_info=True,
        )
