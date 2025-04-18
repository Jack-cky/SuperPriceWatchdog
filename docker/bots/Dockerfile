ARG PYTHON_IMAGE=python:3.10.12-slim-buster

# builder stage
FROM ${PYTHON_IMAGE} AS builder

WORKDIR /app

COPY ../../requirements.txt .

RUN pip install --no-cache-dir --target=packages -r requirements.txt

RUN apt update && apt install -y fontconfig fonts-noto-cjk && fc-cache -fv

# final stage
FROM ${PYTHON_IMAGE}

WORKDIR /superpricewatchdog

RUN apt-get update && apt-get install -y git

COPY --from=builder /app/packages /usr/local/lib/python3.10/site-packages

COPY ../../src/bot.py src/bot.py
COPY ../../superpricewatchdog superpricewatchdog
COPY ../../config/config.ini config/config.ini
COPY --from=builder /usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc config/NotoSansCJK-Bold.ttc

# execution
RUN useradd -m viewer

USER viewer

EXPOSE 5000

CMD ["python", "src/bot.py"]
