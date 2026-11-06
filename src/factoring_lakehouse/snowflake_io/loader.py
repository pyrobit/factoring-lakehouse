"""Load the lakehouse Parquet export into Snowflake, then reconcile against Delta.

Flow per table (one Snowflake transaction):
  1. ``COPY INTO RAW.<table>_LANDING`` from the external stage, scoped to the as-of folder
     (``MATCH_BY_COLUMN_NAME`` makes column order irrelevant; ``FORCE=FALSE`` + load metadata
     prevent double loads).
  2. ``MERGE`` from the landing table into ``RAW.<table>`` on the business key (silver tables), or
     ``DELETE`` + ``INSERT`` of the as-of partition (gold snapshots). Either way reruns are idempotent.
  3. Reconciliation: row count and amount checksum, Snowflake vs. Delta. A mismatch fails the load
     **before** dbt builds marts on top of bad data.

``--dry-run`` prints the SQL without connecting, which is what CI and interview demos use.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from dataclasses import dataclass

from ..config import PipelineConfig, get_logger, load_config

log = get_logger(__name__)


@dataclass(frozen=True)
class TableSpec:
    export_name: str          # folder under the export stage
    target: str               # Snowflake table in RAW
    key: tuple[str, ...]      # business key used by MERGE
    amount_column: str | None  # used for checksum reconciliation
    snapshot: bool = False    # gold tables: replace the as-of partition


TABLES: tuple[TableSpec, ...] = (
    TableSpec("silver_clients", "CLIENTS", ("CLIENT_ID",), "CREDIT_LIMIT"),
    TableSpec("silver_debtors", "DEBTORS_HISTORY", ("DEBTOR_ID", "VALID_FROM"), "CREDIT_LIMIT"),
    TableSpec("silver_invoices", "INVOICES", ("INVOICE_ID",), "AMOUNT"),
    TableSpec("silver_payments", "PAYMENTS", ("PAYMENT_ID",), "AMOUNT"),
    TableSpec("gold_portfolio_snapshot", "PORTFOLIO_SNAPSHOT", ("AS_OF_DATE", "INVOICE_ID"), "OUTSTANDING_AMOUNT",
              snapshot=True),
    TableSpec("gold_debtor_features", "DEBTOR_FEATURES", ("AS_OF_DATE", "DEBTOR_ID"), "OPEN_EXPOSURE",
              snapshot=True),
)


def _ident(name: str) -> str:
    """Allow-list identifiers (we never accept them from user input, but be defensive)."""
    if not name.replace("_", "").replace(".", "").isalnum():
        raise ValueError(f"Unsafe identifier: {name!r}")
    return name.upper()


def copy_sql(spec: TableSpec, stage: str, as_of: dt.date) -> str:
    landing = f"RAW.{_ident(spec.target)}_LANDING"
    return (
        f"CREATE TEMPORARY TABLE IF NOT EXISTS {landing} LIKE RAW.{_ident(spec.target)};\n"
        f"TRUNCATE TABLE {landing};\n"
        f"COPY INTO {landing}\n"
        f"  FROM @{_ident(stage)}/{spec.export_name}/as_of_date={as_of.isoformat()}/\n"
        f"  FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')\n"
        f"  MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE\n"
        f"  ON_ERROR = ABORT_STATEMENT;\n"
        # Gold exports drop the partition column; stamp it back from the folder being loaded.
        + (f"UPDATE {landing} SET AS_OF_DATE = '{as_of.isoformat()}'::DATE;\n" if spec.snapshot else "")
    )


def merge_sql(spec: TableSpec, columns: list[str], as_of: dt.date) -> str:
    target = f"RAW.{_ident(spec.target)}"
    landing = f"{target}_LANDING"
    cols = [_ident(c) for c in columns]
    if spec.snapshot:
        return (
            f"DELETE FROM {target} WHERE AS_OF_DATE = '{as_of.isoformat()}'::DATE;\n"
            f"INSERT INTO {target} ({', '.join(cols)}) SELECT {', '.join(cols)} FROM {landing};"
        )
    on = " AND ".join(f"t.{k} = s.{k}" for k in spec.key)
    updates = ", ".join(f"t.{c} = s.{c}" for c in cols if c not in spec.key)
    return (
        f"MERGE INTO {target} t USING {landing} s ON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(cols)}) VALUES ({', '.join('s.' + c for c in cols)});"
    )


def reconcile_sql(spec: TableSpec, as_of: dt.date) -> str:
    where = f" WHERE AS_OF_DATE = '{as_of.isoformat()}'::DATE" if spec.snapshot else ""
    amount = f"COALESCE(SUM({spec.amount_column}), 0)" if spec.amount_column else "0"
    return f"SELECT COUNT(*) AS ROW_COUNT, {amount} AS AMOUNT_SUM FROM RAW.{_ident(spec.target)}{where};"


@dataclass(frozen=True)
class ReconResult:
    table: str
    source_rows: int
    target_rows: int
    source_amount: float
    target_amount: float
    tolerance: float = 0.01

    @property
    def ok(self) -> bool:
        return self.source_rows == self.target_rows and abs(self.source_amount - self.target_amount) <= self.tolerance


def reconcile(source: dict[str, tuple[int, float]], target: dict[str, tuple[int, float]]) -> list[ReconResult]:
    """Compare (rows, amount) per table. Missing target tables count as zero rows."""
    return [
        ReconResult(t, s_rows, target.get(t, (0, 0.0))[0], float(s_amt), float(target.get(t, (0, 0.0))[1]))
        for t, (s_rows, s_amt) in source.items()
    ]


def manifest_path(cfg: PipelineConfig, as_of: dt.date) -> str:
    return f"{cfg.paths.exports}/_manifest/as_of_date={as_of.isoformat()}/manifest.json"


def build_manifest(spark, cfg: PipelineConfig, as_of: dt.date) -> dict[str, list]:
    """Row count + amount checksum of each exported table, computed by Spark at export time."""
    from pyspark.sql import functions as F

    out = {}
    for spec in TABLES:
        df = spark.read.parquet(f"{cfg.paths.exports}/{spec.export_name}/as_of_date={as_of.isoformat()}")
        row = df.agg(F.count("*").alias("n"),
                     F.coalesce(F.sum(spec.amount_column.lower()), F.lit(0)).alias("s")).first()
        out[spec.target] = [int(row["n"]), float(row["s"])]
    return out


def read_manifest(cfg: PipelineConfig, as_of: dt.date) -> dict[str, tuple[int, float]]:
    """Read the export manifest from local disk or object storage (s3:// via boto3)."""
    path = manifest_path(cfg, as_of)
    if path.startswith("s3://"):  # pragma: no cover - needs AWS
        import boto3

        bucket, key = path[5:].split("/", 1)
        raw = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    else:
        with open(path, "rb") as fh:
            raw = fh.read()
    return {t: (int(v[0]), float(v[1])) for t, v in json.loads(raw).items()}


def plan(cfg: PipelineConfig, as_of: dt.date, columns: dict[str, list[str]] | None = None) -> list[str]:
    """All statements for one load, in order. ``columns`` defaults to a placeholder for dry runs."""
    stage = cfg.snowflake.get("stage", "RAW.LAKEHOUSE_EXPORT_STAGE")
    statements = [f"USE ROLE {cfg.snowflake.get('role', 'LOADER')};",
                  f"USE WAREHOUSE {cfg.snowflake.get('warehouse', 'LOAD_WH')};",
                  f"USE DATABASE {cfg.snowflake.get('database', 'FACTORING')};",
                  "BEGIN;"]
    for spec in TABLES:
        cols = (columns or {}).get(spec.target, [*spec.key, "<other columns resolved at runtime>"])
        statements.append(copy_sql(spec, stage, as_of))
        if columns:
            statements.append(merge_sql(spec, cols, as_of))
        else:
            statements.append(f"-- MERGE INTO RAW.{spec.target} on {', '.join(spec.key)} "
                              f"({'replace as-of partition' if spec.snapshot else 'upsert'})")
    statements.append("COMMIT;")
    statements += [reconcile_sql(spec, as_of) for spec in TABLES]
    return statements


def run(cfg: PipelineConfig, as_of: dt.date) -> list[ReconResult]:  # pragma: no cover - needs Snowflake
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE"),  # key-pair auth, no passwords
        role=cfg.snowflake.get("role"), warehouse=cfg.snowflake.get("warehouse"),
        database=cfg.snowflake.get("database"),
    )
    cur = conn.cursor()
    columns = {}
    for spec in TABLES:
        cur.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA='RAW' "
                    f"AND TABLE_NAME='{spec.target}' ORDER BY ORDINAL_POSITION")
        columns[spec.target] = [r[0] for r in cur.fetchall()]
    try:
        for stmt in plan(cfg, as_of, columns)[: -len(TABLES)]:
            for part in filter(None, (s.strip() for s in stmt.split(";\n"))):
                cur.execute(part)
    except Exception:
        cur.execute("ROLLBACK")
        raise
    target = {}
    for spec in TABLES:
        cur.execute(reconcile_sql(spec, as_of))
        n, s = cur.fetchone()
        target[spec.target] = (int(n), float(s))
    results = reconcile(read_manifest(cfg, as_of), target)
    bad = [r for r in results if not r.ok]
    if bad:
        raise RuntimeError(f"Reconciliation failed: {bad}")
    return results


def main() -> None:
    p = argparse.ArgumentParser(description="Load lakehouse exports into Snowflake")
    p.add_argument("--as-of", required=True, type=dt.date.fromisoformat)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--config")
    args = p.parse_args()
    cfg = load_config(args.config)
    if args.dry_run:
        print("\n".join(plan(cfg, args.as_of)))
        return
    for r in run(cfg, args.as_of):
        log.info(f"step=reconcile table={r.table} ok={r.ok} rows={r.source_rows}/{r.target_rows}")


if __name__ == "__main__":
    main()
