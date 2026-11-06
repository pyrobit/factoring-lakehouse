"""SparkSession factory: Databricks runtime when available, local Delta Lake otherwise."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession


def _on_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_spark(app_name: str = "factoring-lakehouse") -> SparkSession:
    if _on_databricks():
        # Databricks already ships Delta, Photon and a configured session.
        return SparkSession.builder.getOrCreate()

    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Small local tables: fewer Delta log-replay tasks makes local runs/tests much faster.
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.databricks.delta.properties.defaults.checkpointInterval", "100")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        # Allow overwriting a single as_of_date partition without touching the rest.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if os.getenv("SPARK_MASTER", "local[2]").startswith("local"):
        # Local mode needs no resolvable hostname. spark-submit reads these env vars before any
        # Spark conf is applied, so sandboxed runs (`docker run --network none`, CI) start cleanly.
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1").config("spark.driver.host", "localhost")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
