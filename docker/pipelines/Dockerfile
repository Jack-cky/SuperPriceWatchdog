ARG PYTHON_IMAGE=python:3.10.12-slim-buster

FROM ${PYTHON_IMAGE} AS builder

COPY ../../requirements.txt .

RUN pip install --no-cache-dir --target=packages -r requirements.txt

FROM ${PYTHON_IMAGE}

COPY --from=builder packages /usr/local/lib/python3.10/site-packages

WORKDIR /spwd-pipeline

COPY ../../src/pipelines/ src/pipelines

CMD ["python", "src/pipelines/pipeline.py"]
