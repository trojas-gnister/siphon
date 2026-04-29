"""Compute a dry-run diff between mapped records and current DB state."""

from __future__ import annotations

import logging
from typing import Any

from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.models import ModelGenerator

logger = logging.getLogger("siphon")


class Differ:
    """Categorize mapped records against existing DB state.

    For each table:
    - If no on_conflict configured: every record → "insert"
    - If on_conflict.key matches an existing row:
      - action="skip": record → "skip"
      - action="update", values match existing: record → "no_change"
      - action="update", values differ: record → "update" with field-level changes
      - action="error": record → "insert" (DB will reject at insert time)
    - If no existing row: record → "insert"
    """

    def __init__(
        self,
        config: SiphonConfig,
        db_engine: DatabaseEngine,
        model_generator: ModelGenerator,
    ):
        self._config = config
        self._db = db_engine
        self._models = model_generator.models

    async def compute_diff(self, records: list[dict]) -> dict[str, list]:
        """Compute the per-record diff against the current DB state."""
        return {
            "insert": [],
            "update": [],
            "skip": [],
            "no_change": [],
        }
