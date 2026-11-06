"""Typed pipeline configuration loaded from YAML, with environment-variable overrides."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "conf" / "pipeline.yaml"


@dataclass(frozen=True)
class Paths:
    landing: str
    lakehouse: str
    exports: str
    checkpoints: str

    def table(self, layer: str, name: str) -> str:
        """Location of a Delta table, e.g. table('silver', 'invoices')."""
        return f"{self.lakehouse.rstrip('/')}/{layer}/{name}"


@dataclass(frozen=True)
class PipelineConfig:
    env: str
    paths: Paths
    max_reject_rate: dict[str, float]
    concentration_limit: float = 0.25
    late_tolerance_days: int = 0
    feature_lookback_days: int = 90
    snowflake: dict[str, str] = field(default_factory=dict)

    @property
    def is_databricks(self) -> bool:
        return self.env == "databricks"


def load_config(path: str | Path | None = None, **path_overrides: str) -> PipelineConfig:
    """Load config; ``FACTORING_CONFIG`` env var or explicit kwargs override paths (handy in tests)."""
    cfg_path = Path(path or os.getenv("FACTORING_CONFIG", DEFAULT_CONFIG))
    raw = yaml.safe_load(cfg_path.read_text())
    paths = {**raw["paths"], **{k: v for k, v in path_overrides.items() if v}}
    env = os.getenv("FACTORING_ENV", raw.get("env", "local"))
    biz = raw.get("business", {})
    return PipelineConfig(
        env=env,
        paths=Paths(**paths),
        max_reject_rate=raw["quality"]["max_reject_rate"],
        concentration_limit=biz.get("concentration_limit", 0.25),
        late_tolerance_days=biz.get("late_tolerance_days", 0),
        feature_lookback_days=biz.get("feature_lookback_days", 90),
        snowflake=raw.get("snowflake", {}),
    )


def get_logger(name: str) -> logging.Logger:
    """Key=value structured logs, which are easy to parse in Databricks / Airflow log search."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("ts=%(asctime)s level=%(levelname)s logger=%(name)s msg=%(message)s")
        )
        logger.addHandler(handler)
        logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
        logger.propagate = False
    return logger
