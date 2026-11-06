# syntax=docker/dockerfile:1.7
# Airflow image for the local stack: official image + providers + Java 17 + the factoring_lakehouse
# package, so the local DAG can run extraction AND the Spark/Delta pipeline in the worker.
# (In production, heavy compute runs on Databricks; Airflow workers stay thin. See ADR-0007.)
ARG AIRFLOW_VERSION=2.10.3
FROM apache/airflow:${AIRFLOW_VERSION}-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*
USER airflow

ARG AIRFLOW_VERSION=2.10.3
# Providers pinned with the official constraints file for reproducible builds.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      apache-airflow-providers-postgres \
      apache-airflow-providers-mongo \
      apache-airflow-providers-databricks \
      apache-airflow-providers-snowflake \
      apache-airflow-providers-slack \
      apache-airflow-providers-common-sql \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

# The pipeline package (+ PySpark / Delta). Airflow itself stays pinned.
COPY --chown=airflow:root pyproject.toml README.md LICENSE /opt/factoring/
COPY --chown=airflow:root src /opt/factoring/src
COPY --chown=airflow:root conf /opt/factoring/conf
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" /opt/factoring

# Bake the Delta jars into the image (no download when a task starts).
RUN python -c "from factoring_lakehouse.spark_session import get_spark; get_spark('warmup').stop()"

# docker-compose runs Airflow as the host UID (group 0) so bind-mounted ./data stays writable;
# give group 0 write access to the Ivy cache so Spark can resolve the baked jars as that user.
RUN chmod -R g+rwX /home/airflow/.ivy2
