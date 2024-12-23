import hashlib
import hmac
import os
import re
from datetime import datetime
from io import BytesIO

import git
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pytz
import requests
from dotenv import load_dotenv
from flask import Flask, request
from supabase import create_client
from supabase.client import ClientOptions


load_dotenv("./SuperPriceWatchdog/config/.env")


class Config:
    GITHUB_SECRET = os.getenv("GITHUB_SECRET")

app = Flask(__name__)
app.config.from_object(Config)


@app.route("/api/v1/deploy", methods=["POST"])
def git_pull():
    def is_valid_signature(x_hub_signature, data, private_key):
        hash_algorithm, github_signature = x_hub_signature.split("=", 1)
        algorithm = hashlib.__dict__.get(hash_algorithm)
        encoded_key = bytes(private_key, "latin-1")
        mac = hmac.new(encoded_key, msg=data, digestmod=algorithm)
        
        return hmac.compare_digest(mac.hexdigest(), github_signature)
    
    x_hub_signature = request.headers.get("X-Hub-Signature")

    if is_valid_signature(x_hub_signature, request.data, app.config["GITHUB_SECRET"]):
        repo = git.Repo("./SuperPriceWatchdog")
        origin = repo.remotes.origin
        repo.git.reset("--hard", "HEAD")
        repo.git.clean("-fdX")
        repo.create_head("main", origin.refs.main).set_tracking_branch(origin.refs.main).checkout()
        origin.pull()
    
    return "", 200


@app.route("/")
def index():
    
    return (
        "Setup CICD."
    )
