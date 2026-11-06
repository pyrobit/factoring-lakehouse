# syntax=docker/dockerfile:1.7
# Pipeline image: Python 3.11 + Java 17 + PySpark/Delta with the Delta jars baked in, so
# `docker run` needs no network access and nothing installed on the host but Docker.
#
#   docker build -t factoring-lakehouse .
#   docker run --rm factoring-lakehouse                 # make demo
#   docker run --rm factoring-lakehouse make test-all   # pytest + behave

FROM python:3.11.10-slim-bookworm AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VENV=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    TZ=UTC

# Java for Spark (found on PATH, so the image builds on amd64 and arm64), make for the task
# runner, and procps because Spark's launcher scripts call `ps`.
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless make procps \
 && rm -rf /var/lib/apt/lists/*

RUN useradd --create-home --uid 1000 app \
 && python -m venv /opt/venv \
 && chown -R app:app /opt/venv
WORKDIR /app

# ---- dependencies (cached layer: only re-runs when pyproject.toml changes) -----------------
COPY --chown=app:app pyproject.toml README.md ./
RUN mkdir -p src/factoring_lakehouse && touch src/factoring_lakehouse/__init__.py \
 && pip install ".[dev]" \
 && pip uninstall -y factoring-lakehouse

# ---- application ---------------------------------------------------------------------------
COPY --chown=app:app . .
RUN pip install --no-deps -e . && chown -R app:app /app
USER app

# Warm the Ivy cache: download the Delta jars at build time instead of on every container start.
RUN python -c "from factoring_lakehouse.spark_session import get_spark; get_spark('warmup').stop()"

HEALTHCHECK NONE
CMD ["make", "demo"]
