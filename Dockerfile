ARG PYTHON_VERSION=3.10.13
FROM python:${PYTHON_VERSION}-slim as base

WORKDIR /watchdog

RUN apt update && \
    apt install -y fontconfig fonts-noto-cjk && \
    fc-cache -fv

RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=requirements.txt,target=requirements.txt \
    python -m pip install -r requirements.txt

COPY . .

CMD python watchdog.py
