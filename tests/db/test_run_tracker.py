"""Tests for the RunTracker — manages _siphon_runs metadata table."""

from __future__ import annotations

import pytest
from siphon.config.schema import DatabaseConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.run_tracker import RunTracker, SiphonRun


@pytest.fixture
async def engine_setup():
    config = DatabaseConfig(url="sqlite+aiosqlite://")
    engine = DatabaseEngine(config)
    yield engine
    await engine.dispose()


class TestRunTrackerSetup:
    async def test_run_tracker_can_be_constructed(self, engine_setup):
        tracker = RunTracker(engine_setup)
        assert tracker is not None

    async def test_create_runs_table_creates_siphon_runs(self, engine_setup):
        from sqlalchemy import inspect

        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()

        async with engine_setup.engine.connect() as conn:
            def _get_tables(sync_conn):
                inspector = inspect(sync_conn)
                return inspector.get_table_names()
            tables = await conn.run_sync(_get_tables)

        assert "_siphon_runs" in tables

    async def test_create_runs_table_is_idempotent(self, engine_setup):
        """Calling create_runs_table twice doesn't error."""
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        await tracker.create_runs_table()  # should not raise

    async def test_siphon_run_model_columns(self):
        """SiphonRun has the expected columns."""
        cols = {c.name for c in SiphonRun.__table__.columns}
        assert cols == {
            "id", "pipeline_name", "source_file", "started_at",
            "completed_at", "status", "total_records", "processed_count",
            "error_message", "config_hash",
        }
