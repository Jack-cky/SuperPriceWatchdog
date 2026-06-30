FROM python:3.12.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

COPY config/config.ini ./config/config.ini
COPY spwd/__init__.py ./spwd/__init__.py
COPY spwd/pipeline ./spwd/pipeline
COPY spwd/shared ./spwd/shared
COPY pyproject.toml ./

RUN pip install --no-cache-dir --no-compile ".[pipeline]" && \
    useradd --create-home spwd && \
    chown -R spwd:spwd /app

USER spwd

CMD ["python", "-m", "spwd.pipeline.main"]
