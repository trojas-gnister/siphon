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
        result = {
            "insert": [],
            "update": [],
            "skip": [],
            "no_change": [],
        }

        target_table = self._infer_primary_table()
        if target_table is None or not records:
            if records:
                result["insert"] = list(records)
            return result

        table_cfg = self._config.schema_.tables[target_table]

        if table_cfg.on_conflict is None:
            result["insert"] = list(records)
            return result

        on_conflict = table_cfg.on_conflict
        field_to_column = self._field_to_column_map()
        key_columns = [field_to_column[name] for name in on_conflict.key]

        existing = await self._load_existing_rows(target_table, key_columns)

        for record in records:
            key_values = tuple(self._record_value(record, field_to_column, name)
                               for name in on_conflict.key)
            existing_row = existing.get(key_values)

            if existing_row is None:
                result["insert"].append(record)
                continue

            if on_conflict.action == "skip":
                result["skip"].append(record)
                continue

            if on_conflict.action == "error":
                result["insert"].append(record)
                continue

            # action == "update": compare values
            changes = self._compute_changes(
                record, existing_row, field_to_column, on_conflict
            )

            if changes:
                key_dict = {name: record.get(name) for name in on_conflict.key}
                result["update"].append({
                    "key": key_dict,
                    "changes": changes,
                    "record": record,
                })
            else:
                result["no_change"].append(record)

        return result

    def _infer_primary_table(self) -> str | None:
        """Return the first table that has at least one mapped field."""
        for field in self._config.schema_.fields:
            return field.db.table
        return None

    def _field_to_column_map(self) -> dict[str, str]:
        """Map schema field names to their DB column names."""
        mapping = {}
        for f in self._config.schema_.fields:
            mapping[f.name] = f.db.column
        if self._config.schema_.collections:
            for coll in self._config.schema_.collections:
                for f in coll.fields:
                    mapping[f.name] = f.db.column
        return mapping

    @staticmethod
    def _record_value(
        record: dict, field_to_column: dict[str, str], field_name: str
    ) -> Any:
        """Extract a value from a record, supporting both field-name and column-name keys."""
        if field_name in record:
            return record[field_name]
        col_name = field_to_column.get(field_name)
        if col_name and col_name in record:
            return record[col_name]
        return None

    async def _load_existing_rows(
        self, table_name: str, key_columns: list[str]
    ) -> dict[tuple, dict]:
        """Load all existing rows from a table, keyed by the conflict-key tuple."""
        from sqlalchemy import select

        model = self._models[table_name]
        async with self._db.session() as session:
            result = await session.execute(select(model))
            existing: dict[tuple, dict] = {}
            for row in result.scalars():
                key = tuple(getattr(row, col) for col in key_columns)
                row_dict = {col.name: getattr(row, col.name)
                            for col in model.__table__.columns}
                existing[key] = row_dict
        return existing

    def _compute_changes(
        self,
        record: dict,
        existing_row: dict,
        field_to_column: dict[str, str],
        on_conflict,
    ) -> dict[str, dict]:
        """Build a {column: {old, new}} dict for fields that differ.

        Only considers columns covered by `update_columns` (or all non-key
        columns if `update_columns == "all"`).
        """
        key_columns = {field_to_column[name] for name in on_conflict.key}

        if on_conflict.update_columns == "all":
            candidate_fields = [
                f.name for f in self._config.schema_.fields
                if f.db.table == self._infer_primary_table()
                and f.db.column not in key_columns
            ]
        else:
            candidate_fields = list(on_conflict.update_columns)

        changes = {}
        for field_name in candidate_fields:
            col_name = field_to_column.get(field_name, field_name)
            new_value = self._record_value(record, field_to_column, field_name)
            old_value = existing_row.get(col_name)
            if new_value != old_value:
                changes[col_name] = {"old": old_value, "new": new_value}
        return changes
