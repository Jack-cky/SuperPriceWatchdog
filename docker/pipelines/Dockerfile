ARG PYTHON_IMAGE=python:3.10.12-slim-buster

# builder stage
FROM ${PYTHON_IMAGE} AS builder

WORKDIR /app

COPY ../../requirements.txt .

RUN pip install --no-cache-dir --target=packages -r requirements.txt

# final stage
FROM ${PYTHON_IMAGE}

WORKDIR /superpricewatchdog

COPY --from=builder /app/packages /usr/local/lib/python3.10/site-packages

COPY ../../config/config.ini config/config.ini
COPY ../../src/pipeline.py src/pipeline.py

# execution
RUN useradd -m viewer

RUN chown -R viewer:viewer /superpricewatchdog

USER viewer

CMD ["python", "src/pipeline.py"]
