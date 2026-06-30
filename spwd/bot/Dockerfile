FROM python:3.12.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

COPY config/config.ini ./config/config.ini
COPY config/NotoSansCJK-Bold.ttc ./config/NotoSansCJK-Bold.ttc
COPY spwd/__init__.py ./spwd/__init__.py
COPY spwd/bot ./spwd/bot
COPY spwd/shared ./spwd/shared
COPY pyproject.toml ./

RUN pip install --no-cache-dir --no-compile ".[bot]" && \
    useradd --create-home spwd && \
    chown -R spwd:spwd /app

USER spwd

EXPOSE 5000

CMD ["python", "-m", "spwd.bot.main"]
