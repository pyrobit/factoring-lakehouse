"""DAG integrity: imports cleanly, no cycles, expected topology. Skipped when Airflow is not installed
(CI installs it in a dedicated job; see .github/workflows/ci.yml)."""

from pathlib import Path

import pytest

airflow = pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_topology(dagbag):
    dag = dagbag.get_dag("factoring_lakehouse_daily")
    assert dag.max_active_runs == 1
    order = ["extract.postgres_incremental", "databricks_medallion", "snowflake_load.copy_merge",
             "reconcile_delta_vs_snowflake", "dbt.source_freshness", "dbt.build"]
    for upstream, downstream in zip(order, order[1:], strict=False):
        assert downstream in {t.task_id for t in dag.get_task(upstream).get_flat_relatives(upstream=False)}


def test_retries_and_alerting(dagbag):
    dag = dagbag.get_dag("factoring_lakehouse_daily")
    for task in dag.tasks:
        assert task.on_failure_callback is not None, task.task_id
