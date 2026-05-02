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
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                return result.fetchall()
            return []
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


class TestResume:
    async def test_resume_with_no_failed_run_inserts_all(self, tmp_path):
        """resume=True with no prior failed run should behave like a fresh run."""
        config_path = _config_yaml(tmp_path, batch_size=2)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b", "c"])
        config = load_config(config_path)

        result = await Pipeline(config).run(
            csv_path, no_review=True, create_tables=True, resume=True
        )
        assert result.total_inserted == 3

    async def test_resume_skips_already_processed_records(self, tmp_path):
        """A simulated failed run sets processed_count; resume picks up after it."""
        config_path = _config_yaml(tmp_path, batch_size=10)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b", "c", "d"])
        config = load_config(config_path)

        # Bootstrap: ensure the items + _siphon_runs tables exist by running once
        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)
        # Wipe items so the resume actually inserts something
        await _query(config.database.url, "DELETE FROM items")

        # Simulate a previous failed run that processed 2 records
        from siphon.db.engine import DatabaseEngine
        from siphon.db.run_tracker import RunTracker

        config2 = load_config(config_path)
        engine = DatabaseEngine(config2.database)
        tracker = RunTracker(engine)
        await tracker.create_runs_table()
        run_id = await tracker.start_run(
            pipeline_name=config2.name,
            source_file=str(csv_path),
            total_records=4,
            config_hash=Pipeline(config2)._config_hash(),
        )
        await tracker.update_progress(run_id, 2)
        await tracker.fail_run(run_id, "simulated")
        await engine.dispose()

        # Resume — should skip the first 2 and insert the last 2
        result = await Pipeline(load_config(config_path)).run(
            csv_path, no_review=True, resume=True
        )
        assert result.total_inserted == 2

        rows = await _query(config.database.url,
                            "SELECT name FROM items ORDER BY name")
        assert [r[0] for r in rows] == ["c", "d"]
