"""Shared pytest fixtures: one local Spark session, isolated lakehouse paths per test."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")
# PySpark converts TimestampType to naive *local* datetimes; pin UTC so assertions are portable.
os.environ["TZ"] = "UTC"
time.tzset()
os.environ.setdefault("SPARK_MASTER", "local[1]")


@pytest.fixture(scope="session")
def spark():
    from factoring_lakehouse.spark_session import get_spark

    session = get_spark("factoring-lakehouse-tests")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path: Path):
    from factoring_lakehouse.config import load_config

    return load_config(
        landing=str(tmp_path / "landing"),
        lakehouse=str(tmp_path / "lakehouse"),
        exports=str(tmp_path / "exports"),
        checkpoints=str(tmp_path / "checkpoints"),
    )
