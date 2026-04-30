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


class TestRunLifecycle:
    async def test_start_run_returns_id_and_records_running_status(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()

        run_id = await tracker.start_run(
            pipeline_name="test",
            source_file="/tmp/data.csv",
            total_records=10,
            config_hash="abc123",
        )
        assert isinstance(run_id, int)

        run = await tracker.get_run(run_id)
        assert run.status == "running"
        assert run.pipeline_name == "test"
        assert run.source_file == "/tmp/data.csv"
        assert run.total_records == 10
        assert run.processed_count == 0
        assert run.config_hash == "abc123"
        assert run.started_at is not None
        assert run.completed_at is None

    async def test_update_progress_sets_processed_count(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f.csv", 10, "h")

        await tracker.update_progress(run_id, 4)
        run = await tracker.get_run(run_id)
        assert run.processed_count == 4

        await tracker.update_progress(run_id, 7)
        run = await tracker.get_run(run_id)
        assert run.processed_count == 7

    async def test_complete_run_marks_completed(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f.csv", 10, "h")

        await tracker.complete_run(run_id)
        run = await tracker.get_run(run_id)
        assert run.status == "completed"
        assert run.completed_at is not None
        assert run.error_message is None

    async def test_fail_run_marks_failed_with_error(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f.csv", 10, "h")

        await tracker.fail_run(run_id, "boom")
        run = await tracker.get_run(run_id)
        assert run.status == "failed"
        assert run.error_message == "boom"
        assert run.completed_at is not None
