ARG PYTHON_IMAGE=python:3.10.12-slim-bullseye

# builder stage
FROM ${PYTHON_IMAGE} AS builder

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends fontconfig fonts-noto-cjk && \
    fc-cache -fv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt pyproject.toml setup.py ./

RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt && \
    /opt/venv/bin/pip install -e . --no-deps

# final stage
FROM ${PYTHON_IMAGE}

WORKDIR /superpricewatchdog

RUN apt-get update && \
    apt-get install -y --no-install-recommends git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"
ENV POLARS_SKIP_CPU_CHECK=1

COPY --from=builder /usr/share/fonts/opentype/noto/NotoSansCJK-Bold.ttc config/NotoSansCJK-Bold.ttc

COPY config/ config/
COPY src/ src/
COPY superpricewatchdog superpricewatchdog

# execution
RUN useradd -m viewer && \
    chown -R viewer:viewer /superpricewatchdog

USER viewer

EXPOSE 5000

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:5000", "src.bot:app"]
