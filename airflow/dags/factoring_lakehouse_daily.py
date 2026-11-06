"""Daily factoring lakehouse orchestration.

extract (Postgres, Mongo) ──► Databricks medallion job ──► Snowflake COPY+MERGE ──► reconcile gate
                                                                                      │
                                                    publish Dataset ◄── dbt build ◄───┘

Design notes
* Idempotent and backfill-safe: every task is keyed by the logical date ``{{ ds }}``. Silver uses
  MERGE, gold uses partition overwrite, Snowflake uses MERGE / delete+insert, and dbt models are
  incremental with delete+insert.
* ``max_active_runs=1``: MERGE targets are not safe for concurrent writers. Backfills run in order.
* The reconciliation gate stops dbt from publishing marts on top of a partial or duplicated load.
* Connections (``databricks_default``, ``snowflake_default``, ``postgres_factoring``,
  ``mongo_factoring``) come from a secrets backend (AWS Secrets Manager / Azure Key Vault), never
  from the DAG code.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.datasets import Dataset
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.task_group import TaskGroup

LANDING = Variable.get("factoring_landing_root", default_var="s3://factoring-landing-prod/landing")
STATE = Variable.get("factoring_watermark_state", default_var="/opt/airflow/state/watermarks.json")
DBT_DIR = "/opt/airflow/dbt"
MARTS_DATASET = Dataset("snowflake://factoring/marts")


def _alert(context) -> None:
    """Failure callback: route to Slack/PagerDuty via the configured notifier connection."""
    ti = context["task_instance"]
    msg = (f":red_circle: factoring_lakehouse_daily failed: task={ti.task_id} run={context['run_id']} "
           f"try={ti.try_number} log={ti.log_url}")
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id="slack_data_alerts").send(text=msg)
    except Exception:  # alerting must never mask the original failure
        ti.log.exception("Slack alert failed; original error is above")


def _extract_postgres(ds: str, data_interval_end, **_) -> dict:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    from factoring_lakehouse.ingestion.postgres_incremental import ALLOWED_TABLES, extract_table
    from factoring_lakehouse.ingestion.watermark import WatermarkStore

    store = WatermarkStore(STATE)
    conn = PostgresHook(postgres_conn_id="postgres_factoring").get_conn()
    try:
        return {t: extract_table(conn, t, LANDING, ds, store, data_interval_end) for t in ALLOWED_TABLES}
    finally:
        conn.close()


def _extract_mongo(ds: str, data_interval_end, **_) -> int:
    from airflow.providers.mongo.hooks.mongo import MongoHook

    from factoring_lakehouse.ingestion.mongo_events import extract_payments
    from factoring_lakehouse.ingestion.watermark import WatermarkStore

    client = MongoHook(mongo_conn_id="mongo_factoring").get_conn()
    return extract_payments(client["factoring"]["payment_events"], LANDING, ds, WatermarkStore(STATE),
                            data_interval_end)


def _reconcile(ds: str, **context) -> bool:
    """Compare the Databricks export manifest (rows + amount per table) with what landed in Snowflake.

    Returning False short-circuits dbt, so marts are never rebuilt on top of a partial load.
    """
    import datetime as dt

    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    from factoring_lakehouse.config import load_config
    from factoring_lakehouse.snowflake_io.loader import TABLES, read_manifest, reconcile, reconcile_sql

    as_of = dt.date.fromisoformat(ds)
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    target = {s.target: tuple(hook.get_first(reconcile_sql(s, as_of))) for s in TABLES}
    results = reconcile(read_manifest(load_config(), as_of), target)
    for r in results:
        context["ti"].log.info(f"reconcile table={r.table} ok={r.ok} rows={r.source_rows}/{r.target_rows} "
                               f"amount={r.source_amount}/{r.target_amount}")
    return all(r.ok for r in results)


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": _alert,
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="factoring_lakehouse_daily",
    description="Postgres/Mongo -> Databricks (Delta) -> Snowflake -> dbt marts",
    start_date=datetime(2026, 10, 1),
    schedule="0 5 * * *",          # 05:00 UTC, after the ops DB end-of-day close
    catchup=True,                  # safe: every task is idempotent per logical date
    max_active_runs=1,
    default_args=default_args,
    tags=["factoring", "databricks", "snowflake", "dbt"],
    doc_md=__doc__,
    sla_miss_callback=None,
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("extract", tooltip="Sources -> landing zone (JSONL)") as extract:
        pg = PythonOperator(task_id="postgres_incremental", python_callable=_extract_postgres,
                            sla=timedelta(minutes=30))
        mongo = PythonOperator(task_id="mongo_payment_events", python_callable=_extract_mongo)

    databricks = DatabricksRunNowOperator(
        task_id="databricks_medallion",
        databricks_conn_id="databricks_default",
        job_name="factoring-medallion-prod",
        # Sent as-is to the Jobs run-now API (works on every provider version, unlike the newer
        # `job_parameters` argument, which provider 6.12 pinned by Airflow 2.10.3 lacks).
        json={"job_parameters": {"batch_id": "{{ ds }}", "as_of": "{{ ds }}"}},
        deferrable=True,            # frees the worker slot while the cluster runs
        retries=1,
    )

    with TaskGroup("snowflake_load", tooltip="COPY + MERGE into RAW, in one transaction") as sf_load:
        load = BashOperator(
            task_id="copy_merge",
            bash_command="snowflake-load --as-of {{ ds }}",
            env={"FACTORING_ENV": "databricks"},
            append_env=True,
        )
        refresh_pipe = SQLExecuteQueryOperator(
            task_id="refresh_payments_pipe",
            conn_id="snowflake_default",
            sql="alter pipe factoring.raw.payments_pipe refresh",
        )
        load >> refresh_pipe

    reconcile_gate = ShortCircuitOperator(
        task_id="reconcile_delta_vs_snowflake",
        python_callable=_reconcile,
        ignore_downstream_trigger_rules=True,
    )

    with TaskGroup("dbt", tooltip="Snowflake marts") as dbt:
        freshness = BashOperator(task_id="source_freshness",
                                 bash_command=f"cd {DBT_DIR} && dbt source freshness --target prod")
        build = BashOperator(
            task_id="build",
            bash_command=f"cd {DBT_DIR} && dbt build --target prod --vars '{{\"as_of\": \"{{{{ ds }}}}\"}}'",
            outlets=[MARTS_DATASET],   # downstream DAGs (BI extracts, ML training) trigger on this
        )
        freshness >> build

    done = EmptyOperator(task_id="done")

    start >> extract >> databricks >> sf_load >> reconcile_gate >> dbt >> done
