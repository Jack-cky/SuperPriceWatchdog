FROM python:3.12.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_SYSTEM_PYTHON=1 \
    UV_NO_CACHE=1

WORKDIR /app

COPY config/config.ini ./config/config.ini
COPY config/NotoSansCJK-Bold.ttc ./config/NotoSansCJK-Bold.ttc
COPY spwd/__init__.py ./spwd/__init__.py
COPY spwd/bot ./spwd/bot
COPY spwd/shared ./spwd/shared
COPY pyproject.toml ./

RUN --mount=from=ghcr.io/astral-sh/uv:0.10.11,source=/uv,target=/bin/uv \
    uv pip install ".[bot]" && \
    useradd --create-home spwd && \
    chown -R spwd:spwd /app

USER spwd

EXPOSE 5000

CMD ["python", "-m", "spwd.bot.main"]
