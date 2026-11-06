"""Local, fully runnable version of the daily DAG (docker compose --profile airflow).

    extract (Postgres + MongoDB containers) ──► bronze ──► silver ──► gold + export ──► report
                                                                              │
                                                       Snowflake load (dry-run SQL) ◄┘

Differences from ``factoring_lakehouse_daily``: Spark runs in the Airflow worker (local mode)
instead of on a Databricks job cluster, and the Snowflake step prints the COPY/MERGE plan
instead of connecting. Extraction, watermarks, contracts, quality gates and the medallion code are
the same modules that production uses.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

CONFIG = os.getenv("FACTORING_CONFIG", "/opt/factoring/conf/pipeline.airflow-local.yaml")
STATE = "/opt/airflow/data/state/watermarks.json"
# One landing batch per DAG run, so manual re-triggers on the same day never overwrite each other.
BATCH = "{{ run_id | replace(':', '_') | replace('+', '_') }}"
AS_OF = "{{ data_interval_end | ds }}"


def _landing() -> str:
    from factoring_lakehouse.config import load_config

    return load_config(CONFIG).paths.landing


def _extract_postgres(batch: str, data_interval_end, **_) -> dict:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from factoring_lakehouse.ingestion.postgres_incremental import ALLOWED_TABLES, extract_table
    from factoring_lakehouse.ingestion.watermark import WatermarkStore

    store = WatermarkStore(STATE)
    conn = PostgresHook(postgres_conn_id="postgres_factoring").get_conn()
    try:
        return {t: extract_table(conn, t, _landing(), batch, store, data_interval_end) for t in ALLOWED_TABLES}
    finally:
        conn.close()


def _extract_mongo(batch: str, data_interval_end, **_) -> int:
    from airflow.providers.mongo.hooks.mongo import MongoHook

    from factoring_lakehouse.ingestion.mongo_events import extract_payments
    from factoring_lakehouse.ingestion.watermark import WatermarkStore

    client = MongoHook(mongo_conn_id="mongo_factoring").get_conn()
    return extract_payments(client["factoring"]["payment_events"], _landing(), batch, WatermarkStore(STATE),
                            data_interval_end)


def _pipeline(layer: str, with_batch: bool = True) -> str:
    batch = f"--batch {BATCH} " if with_batch else ""
    return f"pipeline {batch}--as-of {AS_OF} --layer {layer}"


with DAG(
    dag_id="factoring_lakehouse_local",
    description="Local stack: Postgres/Mongo containers -> Spark/Delta (local) -> export -> Snowflake dry-run",
    start_date=datetime(2026, 11, 1),
    schedule=None,                 # trigger manually from the UI or `airflow dags trigger`
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["factoring", "local", "demo"],
    doc_md=__doc__,
) as dag:
    env = {"FACTORING_CONFIG": CONFIG, "SPARK_MASTER": "local[2]", "TZ": "UTC"}

    with TaskGroup("extract") as extract:
        PythonOperator(task_id="postgres_incremental", python_callable=_extract_postgres,
                       op_kwargs={"batch": BATCH})
        PythonOperator(task_id="mongo_payment_events", python_callable=_extract_mongo,
                       op_kwargs={"batch": BATCH})

    with TaskGroup("medallion") as medallion:
        bronze = BashOperator(task_id="bronze", bash_command=_pipeline("bronze"), env=env, append_env=True)
        silver = BashOperator(task_id="silver", bash_command=_pipeline("silver"), env=env, append_env=True)
        gold = BashOperator(task_id="gold", bash_command=_pipeline("gold", with_batch=False), env=env,
                            append_env=True)
        export = BashOperator(task_id="export", bash_command=_pipeline("export", with_batch=False), env=env,
                              append_env=True)
        bronze >> silver >> gold >> export

    snowflake_plan = BashOperator(
        task_id="snowflake_load_dry_run",
        bash_command=f"snowflake-load --as-of {AS_OF} --dry-run",
        env=env,
        append_env=True,
    )
    report = BashOperator(task_id="report", bash_command=f"pipeline --report --as-of {AS_OF}", env=env,
                          append_env=True)

    extract >> medallion >> [snowflake_plan, report]
