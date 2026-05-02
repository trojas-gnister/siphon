"""End-to-end test for run tracking and resume."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _write_yaml(tmp_path: Path, db_path: Path, batch_size: int = 2) -> Path:
    yaml = f"""
name: e2e-resume
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


class TestResumeEndToEnd:
    async def test_failed_run_is_recorded_and_resumable(self, tmp_path):
        """Inject a failed run record, then verify --resume picks up correctly."""
        from siphon.db.engine import DatabaseEngine
        from siphon.db.run_tracker import RunTracker

        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path, batch_size=10)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b", "c", "d", "e"])

        # Bootstrap the DB by running once successfully (then clear items)
        config = load_config(config_path)
        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)
        await _query(config.database.url, "DELETE FROM items")
        await _query(config.database.url, "DELETE FROM _siphon_runs")

        # Simulate a failed run that processed 3 records
        config2 = load_config(config_path)
        engine = DatabaseEngine(config2.database)
        tracker = RunTracker(engine)
        await tracker.create_runs_table()
        run_id = await tracker.start_run(
            pipeline_name=config2.name,
            source_file=str(csv_path),
            total_records=5,
            config_hash=Pipeline(config2)._config_hash(),
        )
        await tracker.update_progress(run_id, 3)
        await tracker.fail_run(run_id, "simulated")
        await engine.dispose()

        # Sanity: items table is empty before resume
        items_before = await _query(config.database.url,
                                    "SELECT COUNT(*) FROM items")
        assert items_before[0][0] == 0

        # Resume — should insert the last 2 records (d, e)
        result = await Pipeline(load_config(config_path)).run(
            csv_path, no_review=True, resume=True
        )
        assert result.total_inserted == 2

        names = await _query(config.database.url,
                             "SELECT name FROM items ORDER BY name")
        assert [n[0] for n in names] == ["d", "e"]

        # The resume run should be its own completed _siphon_runs record
        runs = await _query(
            config.database.url,
            "SELECT status, processed_count FROM _siphon_runs ORDER BY id",
        )
        # Two runs: the simulated failed one + the resumed completed one
        assert len(runs) == 2
        assert runs[0][0] == "failed"
        assert runs[0][1] == 3
        assert runs[1][0] == "completed"
        # The completed run's processed_count should be 5 (3 skipped + 2 inserted)
        assert runs[1][1] == 5

    async def test_resume_with_changed_config_hash_does_not_resume(self, tmp_path):
        """If the config changes between runs, the failed run is not resumed."""
        from siphon.db.engine import DatabaseEngine
        from siphon.db.run_tracker import RunTracker

        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path, batch_size=10)
        csv_path = _csv(tmp_path, "data.csv", ["a", "b", "c"])

        config = load_config(config_path)
        # Bootstrap
        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)
        await _query(config.database.url, "DELETE FROM items")
        await _query(config.database.url, "DELETE FROM _siphon_runs")

        engine = DatabaseEngine(config.database)
        tracker = RunTracker(engine)
        await tracker.create_runs_table()
        # Failed run with a different config_hash
        run_id = await tracker.start_run(
            pipeline_name=config.name,
            source_file=str(csv_path),
            total_records=3,
            config_hash="DIFFERENT_HASH",
        )
        await tracker.update_progress(run_id, 1)
        await tracker.fail_run(run_id, "simulated")
        await engine.dispose()

        # Resume should NOT find the old run (different hash) → inserts all 3
        result = await Pipeline(load_config(config_path)).run(
            csv_path, no_review=True, resume=True
        )
        assert result.total_inserted == 3
