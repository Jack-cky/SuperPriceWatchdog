import hashlib
import hmac
import logging
import os

import git
from flask import Blueprint, current_app, request


bp = Blueprint("repository", __name__)


def validate_signature(signature: str, data: bytes) -> bool:
    hash_algorithm, github_signature = signature.split("=", 1)
    algorithm = hashlib.__dict__.get(hash_algorithm)
    encoded_key = bytes(current_app.config["GITHUB_SECRET"], "latin-1")
    mac = hmac.new(encoded_key, msg=data, digestmod=algorithm)

    return hmac.compare_digest(mac.hexdigest(), github_signature)


@bp.route("/api/v1/pull", methods=["POST"])
def pull_from_github() -> tuple[str, int]:
    signature = request.headers.get("X-Hub-Signature")

    if signature and validate_signature(signature, request.data):
        try:
            repo = git.Repo(os.path.dirname(__file__).split(os.sep)[-3])
            origin = repo.remotes.origin

            repo.git.reset("--hard", "HEAD")

            repo.git.pull(origin, "main")
        except Exception as e:
            logging.error("Failed to pull the latest code.", exc_info=True)
            return "", 500
    else:
        logging.warning("Invalid Github webhook signature.")
        return "", 403

    return "", 200
