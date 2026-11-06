"""Bronze layer: land raw records as-is in Delta, with schema enforcement and lineage columns.

* Local / CI: batch read of one landing batch folder.
* Databricks: Auto Loader (``cloudFiles``) incremental stream with ``availableNow`` trigger, so the
  same job behaves like a batch but only picks up files it has not processed yet.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import PipelineConfig, get_logger
from .contracts import CONTRACTS, spark_schema

log = get_logger(__name__)

LINEAGE_COLUMNS = ("_ingested_at", "_source_file", "_batch_id")
TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX"


def add_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(batch_id))
    )


def read_landing(spark: SparkSession, cfg: PipelineConfig, entity: str, batch_id: str) -> DataFrame:
    schema = spark_schema(CONTRACTS[entity])
    path = f"{cfg.paths.landing}/{batch_id}/{entity}"
    df = (
        spark.read.schema(schema)
        .option("mode", "PERMISSIVE")
        # Accepts 'Z' or '+00:00' offsets and 0-6 fractional digits (datagen, Postgres, Mongo).
        .option("timestampFormat", TIMESTAMP_FORMAT)
        .json(path)
    )
    return add_lineage(df, batch_id)


def ingest_batch(spark: SparkSession, cfg: PipelineConfig, entity: str, batch_id: str) -> int:
    """Append one landing batch to bronze. Idempotent: an already-loaded batch_id is replaced."""
    target = cfg.paths.table("bronze", entity)
    df = read_landing(spark, cfg, entity, batch_id)
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_id = '{batch_id}'")
        .partitionBy("_batch_id")
        .save(target)
    )
    count = spark.read.format("delta").load(target).where(F.col("_batch_id") == batch_id).count()
    log.info(f"layer=bronze entity={entity} batch={batch_id} rows={count}")
    return count


def ingest_autoloader(spark: SparkSession, cfg: PipelineConfig, entity: str) -> None:
    """Databricks-only: incremental file discovery with Auto Loader (exactly-once via checkpoint)."""
    schema = spark_schema(CONTRACTS[entity])
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # unknown fields -> _rescued_data
        .schema(schema)
        .load(f"{cfg.paths.landing}/*/{entity}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.regexp_extract("_metadata.file_path", r"/([^/]+)/" + entity + "/", 1))
    )
    (
        stream.writeStream.format("delta")
        .option("checkpointLocation", f"{cfg.paths.checkpoints}/bronze_{entity}")
        .partitionBy("_batch_id")
        .trigger(availableNow=True)
        .start(cfg.paths.table("bronze", entity))
        .awaitTermination()
    )
