ARG PYTHON_IMAGE=python:3.10.12-slim-buster

FROM ${PYTHON_IMAGE} AS builder

COPY ../../requirements.txt .

RUN pip install --no-cache-dir --target=packages -r requirements.txt

RUN apt update && apt install -y fontconfig fonts-noto-cjk && fc-cache -fv

FROM ${PYTHON_IMAGE}

RUN apt-get update && apt-get install -y git

COPY --from=builder packages /usr/local/lib/python3.10/site-packages

WORKDIR /spwd-bot

COPY ../../src/bots/bot.py src/bots/bot.py
COPY ../../templates/index.html templates/index.html
COPY --from=builder /usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc SuperPriceWatchdog/config/NotoSansCJK-Bold.ttc

EXPOSE 5000

CMD ["python", "src/bots/bot.py"]
