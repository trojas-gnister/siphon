"""End-to-end test for the audit trail."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _write_yaml(tmp_path: Path, db_path: Path) -> Path:
    yaml = f"""
name: e2e-audit
source: {{ type: spreadsheet }}
database: {{ url: "sqlite+aiosqlite:///{db_path}" }}
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: {{ table: items, column: name }}
    - name: phone
      source: "Phone"
      type: string
      db: {{ table: items, column: phone }}
  tables:
    items:
      primary_key: {{ column: id, type: auto_increment }}
      on_conflict:
        key: [name]
        action: update
        update_columns: all
pipeline:
  review: false
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _csv(tmp_path: Path, name: str, rows: list[dict]) -> Path:
    p = tmp_path / name
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row[h]) for h in headers))
    p.write_text("\n".join(lines) + "\n")
    return p


async def _query(db_url: str, sql: str):
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                return result.fetchall()
            return []
    finally:
        await engine.dispose()


class TestAuditEndToEnd:
    async def test_full_audit_trail_for_insert_and_update(self, tmp_path):
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)

        # First run: insert two records as alice
        first = _csv(tmp_path, "first.csv", [
            {"Name": "Acme", "Phone": "OLD"},
            {"Name": "Beta", "Phone": "111"},
        ])
        await Pipeline(load_config(config_path)).run(
            first, no_review=True, create_tables=True, user="alice"
        )

        # Second run: update Acme as bob (Beta unchanged, Gamma new)
        second = _csv(tmp_path, "second.csv", [
            {"Name": "Acme", "Phone": "NEW"},
            {"Name": "Beta", "Phone": "111"},
            {"Name": "Gamma", "Phone": "333"},
        ])
        await Pipeline(load_config(config_path)).run(
            second, no_review=True, user="bob"
        )

        # Audit trail should contain:
        # - 2 inserts by alice (Acme, Beta) for run 1
        # - 1 update by bob (Acme: OLD->NEW) and 1 insert (Gamma) for run 2
        # - Beta's no-op update is NOT audited
        rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT run_id, action, target_table, reviewed_by, field_changes "
            "FROM _siphon_audit ORDER BY id",
        )

        # Run 1: 2 inserts by alice
        run1_rows = [r for r in rows if r[0] == 1]
        assert len(run1_rows) == 2
        assert all(r[1] == "insert" for r in run1_rows)
        assert all(r[3] == "alice" for r in run1_rows)

        # Run 2: 1 update + 1 insert by bob
        run2_rows = [r for r in rows if r[0] == 2]
        assert len(run2_rows) == 2
        actions = sorted(r[1] for r in run2_rows)
        assert actions == ["insert", "update"]
        assert all(r[3] == "bob" for r in run2_rows)

        # The update should have field_changes with phone OLD->NEW
        update_row = next(r for r in run2_rows if r[1] == "update")
        changes = json.loads(update_row[4])
        assert changes == {"phone": {"old": "OLD", "new": "NEW"}}

    async def test_source_file_recorded_in_audit(self, tmp_path):
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)

        csv_path = _csv(tmp_path, "import.csv", [{"Name": "X", "Phone": "1"}])
        await Pipeline(load_config(config_path)).run(
            csv_path, no_review=True, create_tables=True
        )

        rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT source_file FROM _siphon_audit",
        )
        assert str(csv_path) in rows[0][0]
