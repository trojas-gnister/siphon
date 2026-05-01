"""Pipeline tests for run tracking and resume."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _config_yaml(tmp_path: Path, batch_size: int = 500,
                 track_runs: bool = True) -> Path:
    db_path = tmp_path / "test.db"
    yaml = f"""
name: resume-test
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
  batch_size: {batch_size}
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
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            return result.fetchall()
    finally:
        await engine.dispose()


class TestRunTrackingEnabled:
    async def test_successful_run_creates_completed_record(self, tmp_path):
        config_path = _config_yaml(tmp_path, batch_size=2)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b", "c"])
        config = load_config(config_path)

        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)

        rows = await _query(config.database.url,
                            "SELECT pipeline_name, status, total_records, processed_count "
                            "FROM _siphon_runs")
        assert len(rows) == 1
        assert rows[0][0] == "resume-test"
        assert rows[0][1] == "completed"
        assert rows[0][2] == 3
        assert rows[0][3] == 3

    async def test_dry_run_does_not_create_run_record(self, tmp_path):
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b"])
        config = load_config(config_path)

        # Set up the DB so the dry-run can read from it
        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)
        before = await _query(config.database.url, "SELECT COUNT(*) FROM _siphon_runs")
        await Pipeline(config).run(csv_path, dry_run=True, no_review=True)
        after = await _query(config.database.url, "SELECT COUNT(*) FROM _siphon_runs")

        assert before[0][0] == after[0][0]


class TestRunTrackingDisabled:
    async def test_track_runs_false_does_not_create_table(self, tmp_path):
        from sqlalchemy import inspect

        config_path = _config_yaml(tmp_path, track_runs=False)
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

        assert "_siphon_runs" not in tables
