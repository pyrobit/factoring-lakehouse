"""Data-quality framework: contract-derived rules, quarantine split, reject-rate gate, metrics.

Each row gets a ``_dq_failures`` array listing every rule it broke. Rows with an empty array are
*valid*. All others go to ``quarantine/<entity>`` with the reasons, so nothing is silently dropped
and data stewards can replay rows once the source is fixed.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .config import PipelineConfig, get_logger
from .contracts import CONTRACTS, RuleSpec, derive_rules

log = get_logger(__name__)


class DataQualityError(RuntimeError):
    """Raised when an entity's reject rate exceeds its configured threshold."""


@dataclass
class DQResult:
    entity: str
    valid: DataFrame
    rejected: DataFrame
    total: int
    rejected_count: int
    rule_failures: dict[str, int] = field(default_factory=dict)

    @property
    def reject_rate(self) -> float:
        return self.rejected_count / self.total if self.total else 0.0


def _failure_condition(rule: RuleSpec) -> Column:
    """A boolean Column that is True when the row *violates* the rule."""
    c = F.col(rule.column)
    if rule.kind == "not_null":
        return c.isNull()
    if rule.kind == "accepted_values":
        return c.isNotNull() & ~c.isin(rule.params["values"])
    if rule.kind == "range":
        p = rule.params
        cond = F.lit(False)
        if "ge" in p:
            cond = cond | (c < F.lit(p["ge"]))
        if "gt" in p:
            cond = cond | (c <= F.lit(p["gt"]))
        if "le" in p:
            cond = cond | (c > F.lit(p["le"]))
        if "lt" in p:
            cond = cond | (c >= F.lit(p["lt"]))
        return c.isNotNull() & cond
    if rule.kind == "length":
        p = rule.params
        return c.isNotNull() & (
            (F.length(c) < p.get("min_length", 0)) | (F.length(c) > p.get("max_length", 1 << 30))
        )
    if rule.kind == "unique":
        return F.count(F.lit(1)).over(Window.partitionBy(c)) > 1
    raise ValueError(f"Unsupported rule kind for column check: {rule.kind}")


def apply_rules(
    df: DataFrame,
    rules: Iterable[RuleSpec],
    references: dict[str, DataFrame] | None = None,
) -> DataFrame:
    """Return ``df`` with a ``_dq_failures`` array column.

    ``references`` maps an entity name to a DataFrame holding the valid keys for foreign-key rules.
    Missing references skip that FK rule, so a rule is never treated as passing by accident: the
    caller decides what reference data is authoritative.
    """
    references = references or {}
    failure_flags: list[Column] = []
    helper_cols: list[str] = []
    for rule in rules:
        if rule.kind == "foreign_key":
            ref = references.get(rule.params["entity"])
            if ref is None:
                continue
            flag_col = f"__fk_{rule.name}"
            keys = ref.select(F.col(rule.params["column"]).alias(flag_col)).distinct()
            df = df.join(keys, df[rule.column] == keys[flag_col], "left")
            failure_flags.append(F.when(F.col(rule.column).isNotNull() & F.col(flag_col).isNull(), F.lit(rule.name)))
            helper_cols.append(flag_col)
        else:
            failure_flags.append(F.when(_failure_condition(rule), F.lit(rule.name)))
    failures = F.array_compact(F.array(*failure_flags)) if failure_flags else F.array().cast("array<string>")
    return df.withColumn("_dq_failures", failures).drop(*helper_cols)


def split(
    df: DataFrame,
    entity: str,
    references: dict[str, DataFrame] | None = None,
    extra_rules: Iterable[RuleSpec] = (),
) -> DQResult:
    rules = [*derive_rules(CONTRACTS[entity]), *extra_rules]
    checked = apply_rules(df, rules, references).cache()
    total = checked.count()
    rejected = checked.where(F.size("_dq_failures") > 0)
    rule_failures = {
        r["rule"]: r["n"]
        for r in rejected.select(F.explode("_dq_failures").alias("rule")).groupBy("rule").count()
        .withColumnRenamed("count", "n").collect()
    }
    rejected_count = rejected.count()
    valid = checked.where(F.size("_dq_failures") == 0).drop("_dq_failures")
    log.info(f"layer=quality entity={entity} total={total} rejected={rejected_count} rules={rule_failures}")
    return DQResult(entity, valid, rejected, total, rejected_count, rule_failures)


def enforce_threshold(result: DQResult, cfg: PipelineConfig) -> None:
    limit = cfg.max_reject_rate.get(result.entity, 0.0)
    if result.reject_rate > limit:
        raise DataQualityError(
            f"{result.entity}: reject rate {result.reject_rate:.2%} exceeds limit {limit:.2%} "
            f"(failures={result.rule_failures})"
        )


def write_quarantine(spark: SparkSession, cfg: PipelineConfig, result: DQResult, batch_id: str) -> None:
    """Replace this batch's quarantine slice (idempotent) and append run metrics."""
    target = cfg.paths.table("quarantine", result.entity)
    (
        result.rejected.withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(batch_id))
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"_batch_id = '{batch_id}'")
        .option("mergeSchema", "true")
        .partitionBy("_batch_id")
        .save(target)
    )
    metrics = spark.createDataFrame(
        [(batch_id, result.entity, result.total, result.rejected_count, result.reject_rate,
          {k: int(v) for k, v in result.rule_failures.items()})],
        "batch_id string, entity string, total long, rejected long, reject_rate double, "
        "rule_failures map<string,long>",
    ).withColumn("measured_at", F.current_timestamp())
    (
        metrics.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"batch_id = '{batch_id}' AND entity = '{result.entity}'")
        .save(cfg.paths.table("ops", "dq_metrics"))
    )
