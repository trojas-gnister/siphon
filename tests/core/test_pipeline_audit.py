"""Pipeline tests for audit trail integration."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _config_yaml(tmp_path: Path, audit: bool = True, track_runs: bool = True) -> Path:
    db_path = tmp_path / "test.db"
    yaml = f"""
name: audit-test
source: {{ type: spreadsheet }}
database: {{ url: "sqlite+aiosqlite:///{db_path}" }}
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: {{ table: items, column: name }}
  tables:
    items:
      primary_key: {{ column: id, type: auto_increment }}
pipeline:
  review: false
  audit: {str(audit).lower()}
  track_runs: {str(track_runs).lower()}
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _csv(tmp_path: Path, name: str, names: list[str]) -> Path:
    p = tmp_path / name
    p.write_text("Name\n" + "\n".join(names) + "\n")
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


class TestPipelineAudit:
    async def test_successful_run_writes_audit_entries(self, tmp_path):
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b", "c"])
        config = load_config(config_path)

        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)

        rows = await _query(
            config.database.url,
            "SELECT target_table, action, source_file FROM _siphon_audit ORDER BY id",
        )
        assert len(rows) == 3
        assert all(r[0] == "items" for r in rows)
        assert all(r[1] == "insert" for r in rows)
        assert all(str(csv_path) in r[2] for r in rows)

    async def test_user_flag_recorded_in_audit(self, tmp_path):
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", ["a"])
        config = load_config(config_path)

        await Pipeline(config).run(
            csv_path, no_review=True, create_tables=True, user="alice"
        )

        rows = await _query(
            config.database.url,
            "SELECT reviewed_by FROM _siphon_audit",
        )
        assert rows[0][0] == "alice"

    async def test_audit_disabled_does_not_create_table(self, tmp_path):
        from sqlalchemy import inspect

        config_path = _config_yaml(tmp_path, audit=False)
        csv_path = _csv(tmp_path, "data.csv", ["a"])
        config = load_config(config_path)

        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)

        engine = create_async_engine(config.database.url)
        try:
            async with engine.connect() as conn:
                def _get_tables(sync_conn):
                    return inspect(sync_conn).get_table_names()
                tables = await conn.run_sync(_get_tables)
        finally:
            await engine.dispose()

        assert "_siphon_audit" not in tables

    async def test_audit_requires_track_runs(self, tmp_path):
        """Audit needs run_id; if track_runs=false, audit is silently skipped."""
        from sqlalchemy import inspect

        config_path = _config_yaml(tmp_path, audit=True, track_runs=False)
        csv_path = _csv(tmp_path, "data.csv", ["a"])
        config = load_config(config_path)

        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)

        engine = create_async_engine(config.database.url)
        try:
            async with engine.connect() as conn:
                def _get_tables(sync_conn):
                    return inspect(sync_conn).get_table_names()
                tables = await conn.run_sync(_get_tables)
        finally:
            await engine.dispose()

        assert "_siphon_audit" not in tables

    async def test_dry_run_does_not_write_audit(self, tmp_path):
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", ["a"])
        config = load_config(config_path)

        # Bootstrap so subsequent dry-run can read the DB
        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)
        await _query(config.database.url, "DELETE FROM items")
        await _query(config.database.url, "DELETE FROM _siphon_audit")

        await Pipeline(config).run(csv_path, dry_run=True, no_review=True)

        rows = await _query(config.database.url, "SELECT COUNT(*) FROM _siphon_audit")
        assert rows[0][0] == 0
