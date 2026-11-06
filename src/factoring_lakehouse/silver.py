"""Silver layer: conformed, deduplicated, quality-checked entities, kept current with Delta MERGE.

Load patterns:
  clients   Type-1 upsert (newest ``updated_at`` wins)
  debtors   SCD Type-2 history (``valid_from`` / ``valid_to`` / ``is_current`` / ``_row_hash``)
  invoices  Type-1 upsert guarded by ``updated_at``, so late-arriving stale CDC events are ignored
  payments  insert-only (immutable facts), deduplicated on ``payment_id``

Every merge is idempotent: re-running a batch yields the same table state.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import quality
from .bronze import LINEAGE_COLUMNS
from .config import PipelineConfig, get_logger
from .contracts import CONTRACTS

log = get_logger(__name__)

SCD2_COLUMNS = ("valid_from", "valid_to", "is_current", "_row_hash")


@dataclass
class SilverStats:
    entity: str
    input_rows: int = 0
    deduplicated: int = 0
    rejected: int = 0
    merged: int = 0
    extra: dict = field(default_factory=dict)


# --------------------------------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------------------------------


def latest_per_key(df: DataFrame, key: str, order_col: str = "updated_at") -> DataFrame:
    """Keep the newest version of each key. Rows with a NULL key are passed through untouched,
    so the quality layer can quarantine them (instead of collapsing them into one row)."""
    w = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last(), F.col("_ingested_at").desc())
    keyed = df.where(F.col(key).isNotNull()).withColumn("__rn", F.row_number().over(w))
    return keyed.where("__rn = 1").drop("__rn").unionByName(df.where(F.col(key).isNull()))


def row_hash(columns: list[str]) -> F.Column:
    return F.sha2(F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in columns]), 256)


def _table_exists(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def read_silver(spark: SparkSession, cfg: PipelineConfig, entity: str, current_only: bool = True) -> DataFrame:
    df = spark.read.format("delta").load(cfg.paths.table("silver", entity))
    if entity == "debtors" and current_only:
        df = df.where("is_current")
    return df


def _bronze_batch(spark: SparkSession, cfg: PipelineConfig, entity: str, batch_id: str) -> DataFrame:
    return (
        spark.read.format("delta").load(cfg.paths.table("bronze", entity))
        .where(F.col("_batch_id") == batch_id)
        .drop("_source_file")
    )


# --------------------------------------------------------------------------------------------------
# merge strategies
# --------------------------------------------------------------------------------------------------


def merge_upsert(spark: SparkSession, df: DataFrame, path: str, key: str, guard_col: str | None) -> dict:
    """Type-1 upsert. With ``guard_col`` a matched row is only overwritten by a *newer* version."""
    if not _table_exists(spark, path):
        df.write.format("delta").save(path)
        return {"inserted": df.count(), "updated": 0, "stale_skipped": 0}

    target = DeltaTable.forPath(spark, path)
    stale = 0
    if guard_col:
        stale = (
            df.alias("s").join(target.toDF().alias("t"), key)
            .where(F.col(f"s.{guard_col}") < F.col(f"t.{guard_col}"))  # equal = harmless replay
            .count()
        )
    condition = f"s.{guard_col} > t.{guard_col}" if guard_col else None
    (
        target.alias("t")
        .merge(df.alias("s"), f"t.{key} = s.{key}")
        .whenMatchedUpdateAll(condition=condition)
        .whenNotMatchedInsertAll()
        .execute()
    )
    m = _last_merge_metrics(target)
    return {"inserted": m.get("numTargetRowsInserted", 0), "updated": m.get("numTargetRowsUpdated", 0),
            "stale_skipped": stale}


def merge_insert_only(spark: SparkSession, df: DataFrame, path: str, key: str) -> dict:
    if not _table_exists(spark, path):
        df.write.format("delta").save(path)
        return {"inserted": df.count()}
    target = DeltaTable.forPath(spark, path)
    target.alias("t").merge(df.alias("s"), f"t.{key} = s.{key}").whenNotMatchedInsertAll().execute()
    return {"inserted": _last_merge_metrics(target).get("numTargetRowsInserted", 0)}


def merge_scd2(spark: SparkSession, df: DataFrame, path: str, key: str, tracked: list[str],
               effective_col: str = "updated_at") -> dict:
    """SCD Type-2 in a single atomic MERGE (the "staged union" pattern).

    Each changed key appears twice in the staged source:
      * with ``_merge_key = key``  -> matches the current row and closes it (``valid_to``)
      * with ``_merge_key = NULL`` -> never matches, so it is inserted as the new current version
    Unchanged keys (same ``_row_hash``) match but fail the update condition, so they are a no-op.
    """
    src = df.withColumn("_row_hash", row_hash(tracked))
    if not _table_exists(spark, path):
        (
            src.withColumn("valid_from", F.col(effective_col))
            .withColumn("valid_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .write.format("delta").save(path)
        )
        return {"inserted": src.count(), "closed": 0}

    target = DeltaTable.forPath(spark, path)
    current = target.toDF().where("is_current").select(key, F.col("_row_hash").alias("__t_hash"),
                                                         F.col("valid_from").alias("__t_from"))
    changed = (
        src.join(current, key)
        .where((F.col("_row_hash") != F.col("__t_hash")) & (F.col(effective_col) > F.col("__t_from")))
        .drop("__t_hash", "__t_from")
    )
    staged = changed.withColumn("_merge_key", F.lit(None).cast("string")).unionByName(
        src.withColumn("_merge_key", F.col(key))
    )
    insert_values = {c: f"s.{c}" for c in src.columns}
    insert_values |= {"valid_from": f"s.{effective_col}", "valid_to": "CAST(NULL AS TIMESTAMP)",
                      "is_current": "true"}
    (
        target.alias("t")
        .merge(staged.alias("s"), f"t.{key} = s._merge_key AND t.is_current = true")
        .whenMatchedUpdate(
            condition=f"t._row_hash <> s._row_hash AND s.{effective_col} > t.valid_from",
            set={"is_current": "false", "valid_to": f"s.{effective_col}"},
        )
        # Brand-new keys arrive with a non-null _merge_key that matches nothing -> insert.
        # Changed keys arrive a second time with a NULL _merge_key -> insert the new version.
        # Unchanged replays have a non-null _merge_key that matched -> nothing happens.
        .whenNotMatchedInsert(values=insert_values)
        .execute()
    )
    m = _last_merge_metrics(target)
    return {"inserted": m.get("numTargetRowsInserted", 0), "closed": m.get("numTargetRowsUpdated", 0)}


def _last_merge_metrics(target: DeltaTable) -> dict:
    row = target.history(1).select("operationMetrics").first()
    return {k: int(v) for k, v in (row["operationMetrics"] or {}).items() if v and v.isdigit()}


# --------------------------------------------------------------------------------------------------
# entity loaders
# --------------------------------------------------------------------------------------------------


def _prepare(spark: SparkSession, cfg: PipelineConfig, entity: str, batch_id: str,
             references: dict[str, DataFrame] | None = None) -> tuple[DataFrame, SilverStats]:
    key = CONTRACTS[entity].__primary_key__
    order_col = "event_ts" if entity == "payments" else "updated_at"
    raw = _bronze_batch(spark, cfg, entity, batch_id)
    stats = SilverStats(entity, input_rows=raw.count())
    deduped = latest_per_key(raw, key, order_col)
    stats.deduplicated = stats.input_rows - deduped.count()

    result = quality.split(deduped, entity, references)
    quality.write_quarantine(spark, cfg, result, batch_id)
    quality.enforce_threshold(result, cfg)
    stats.rejected = result.rejected_count
    return result.valid, stats


def load_clients(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> SilverStats:
    valid, stats = _prepare(spark, cfg, "clients", batch_id)
    stats.extra = merge_upsert(spark, valid, cfg.paths.table("silver", "clients"), "client_id", "updated_at")
    return stats


def load_debtors(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> SilverStats:
    valid, stats = _prepare(spark, cfg, "debtors", batch_id)
    tracked = [c for c in CONTRACTS["debtors"].model_fields if c not in ("debtor_id", "updated_at")]
    stats.extra = merge_scd2(spark, valid, cfg.paths.table("silver", "debtors"), "debtor_id", tracked)
    return stats


def load_invoices(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> SilverStats:
    refs = {"clients": read_silver(spark, cfg, "clients"), "debtors": read_silver(spark, cfg, "debtors")}
    valid, stats = _prepare(spark, cfg, "invoices", batch_id, refs)
    stats.extra = merge_upsert(spark, valid, cfg.paths.table("silver", "invoices"), "invoice_id", "updated_at")
    return stats


def load_payments(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> SilverStats:
    refs = {"invoices": read_silver(spark, cfg, "invoices")}
    valid, stats = _prepare(spark, cfg, "payments", batch_id, refs)
    stats.extra = merge_insert_only(spark, valid, cfg.paths.table("silver", "payments"), "payment_id")
    return stats


LOADERS = {"clients": load_clients, "debtors": load_debtors, "invoices": load_invoices, "payments": load_payments}


def run(spark: SparkSession, cfg: PipelineConfig, batch_id: str) -> list[SilverStats]:
    """Load all entities in dependency order (dimensions before facts, for FK checks)."""
    out = []
    for entity in ("clients", "debtors", "invoices", "payments"):
        stats = LOADERS[entity](spark, cfg, batch_id)
        log.info(f"layer=silver entity={entity} batch={batch_id} input={stats.input_rows} "
                 f"dedup={stats.deduplicated} rejected={stats.rejected} merge={stats.extra}")
        out.append(stats)
    return out


__all__ = ["run", "merge_scd2", "merge_upsert", "merge_insert_only", "latest_per_key", "LINEAGE_COLUMNS"]
