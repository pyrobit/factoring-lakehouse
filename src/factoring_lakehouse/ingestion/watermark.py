"""Watermark state for incremental extraction.

Local runs keep state in a JSON file. In Airflow the same interface is backed by an Airflow
Variable or a control table (see ``airflow/dags``). The watermark only advances **after** the batch
file is durably written, which gives at-least-once delivery; silver's MERGE makes replays harmless.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class WatermarkStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def _load(self) -> dict[str, str]:
        return json.loads(self.path.read_text()) if self.path.exists() else {}

    def get(self, source: str) -> datetime:
        raw = self._load().get(source)
        return datetime.fromisoformat(raw) if raw else EPOCH

    def set(self, source: str, value: datetime) -> None:
        current = self.get(source)
        if value < current:
            raise ValueError(f"Watermark for {source} cannot move backwards ({value} < {current})")
        state = self._load()
        state[source] = value.isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=2))
        tmp.replace(self.path)  # atomic rename
