"""behave hooks: one Spark session for the whole run, a fresh lakehouse directory per scenario."""

from __future__ import annotations

import os
import shutil
import tempfile
import time

os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
# PySpark converts TimestampType to naive *local* datetimes; pin UTC so assertions are portable.
os.environ["TZ"] = "UTC"
time.tzset()
os.environ.setdefault("SPARK_MASTER", "local[1]")


def before_all(context):
    from factoring_lakehouse.spark_session import get_spark

    context.spark = get_spark("factoring-lakehouse-bdd")


def before_scenario(context, scenario):
    from factoring_lakehouse.config import load_config

    context.workdir = tempfile.mkdtemp(prefix="bdd-")
    context.cfg = load_config(
        landing=f"{context.workdir}/landing",
        lakehouse=f"{context.workdir}/lakehouse",
        exports=f"{context.workdir}/exports",
        checkpoints=f"{context.workdir}/chk",
    )


def after_scenario(context, scenario):
    shutil.rmtree(context.workdir, ignore_errors=True)


def after_all(context):
    context.spark.stop()
