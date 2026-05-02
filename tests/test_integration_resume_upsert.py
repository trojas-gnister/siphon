"""Resume + upsert idempotency.

When a pipeline run fails partway through and is resumed, records before
the failure point are already in the DB. With on_conflict.action=update,
re-processing them should be a no-op (or produce the same final state).

Without upserts, re-processing would either fail on unique-key conflict OR
create duplicates. With upserts, resume should be safe.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _write_yaml(tmp_path: Path, db_path: Path) -> Path:
    yaml = f"""
name: resume-upsert
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
  batch_size: 5
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


class TestResumeWithUpsert:
    async def test_resume_reprocesses_safely_with_upsert(self, tmp_path):
        """Simulating: run fails after 3 of 6 records committed.
        Resume re-runs records 4-6. With on_conflict=update, this is safe.

        After resume, all 6 records should be in DB exactly once.
        """
        from siphon.db.engine import DatabaseEngine
        from siphon.db.run_tracker import RunTracker

        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)
        csv_path = _csv(tmp_path, "data.csv", [
            {"Name": "a", "Phone": "1"},
            {"Name": "b", "Phone": "2"},
            {"Name": "c", "Phone": "3"},
            {"Name": "d", "Phone": "4"},
            {"Name": "e", "Phone": "5"},
            {"Name": "f", "Phone": "6"},
        ])

        # Bootstrap: run normally to create tables
        config = load_config(config_path)
        await Pipeline(config).run(csv_path, no_review=True, create_tables=True)
        # Wipe items + audit + runs so resume actually has work to do
        await _query(config.database.url, "DELETE FROM items")
        await _query(config.database.url, "DELETE FROM _siphon_runs")
        await _query(config.database.url, "DELETE FROM _siphon_audit")

        # Pretend an earlier run inserted a, b, c then failed
        config2 = load_config(config_path)
        engine = DatabaseEngine(config2.database)
        tracker = RunTracker(engine)
        await tracker.create_runs_table()
        # Manually insert a, b, c to simulate the failed run's partial commit
        await _query(config2.database.url,
                     "INSERT INTO items (name, phone) VALUES "
                     "('a', '1'), ('b', '2'), ('c', '3')")
        # Record the failed run
        run_id = await tracker.start_run(
            pipeline_name=config2.name,
            source_file=str(csv_path),
            total_records=6,
            config_hash=Pipeline(config2)._config_hash(),
        )
        await tracker.update_progress(run_id, 3)
        await tracker.fail_run(run_id, "simulated")
        await engine.dispose()

        # Resume — should skip first 3, insert d, e, f
        result = await Pipeline(load_config(config_path)).run(
            csv_path, no_review=True, resume=True,
        )

        assert result.total_inserted == 3

        # All 6 records present, no duplicates
        rows = await _query(config.database.url,
                            "SELECT name, phone FROM items ORDER BY name")
        names_phones = [(r[0], r[1]) for r in rows]
        assert names_phones == [
            ("a", "1"), ("b", "2"), ("c", "3"),
            ("d", "4"), ("e", "5"), ("f", "6"),
        ]
