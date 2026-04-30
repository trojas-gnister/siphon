# Siphon v3 Phase 3: Incremental/Resumable Runs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Track each pipeline run in a `_siphon_runs` metadata table so failed runs can be resumed from where they stopped instead of starting over.

**Architecture:** A `RunTracker` class manages the lifecycle of run records (start/update/complete/fail) in a Siphon-managed `_siphon_runs` table. The `Inserter` gains batched commits with a progress callback. The pipeline computes a config hash, starts a run before insertion, updates progress per batch, and finalizes the run. With `--resume`, the pipeline finds the last failed run for the same pipeline+source+config and skips records up to its `processed_count`.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.0 async, Pydantic 2.0, hashlib (stdlib), pytest, aiosqlite (test).

**Spec:** `docs/superpowers/specs/2026-04-22-siphon-v3-roadmap.md` (Phase 3 section)

**Branch:** Cut `v3-phase-3-resumable-runs` from `v3-phase-2-dry-run-diff`.

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/db/run_tracker.py` | Manage `_siphon_runs` table + run lifecycle | Create |
| `siphon/db/inserter.py` | Batched commits with progress callback | Modify |
| `siphon/config/schema.py` | Add `batch_size` and `track_runs` to `PipelineConfig` | Modify |
| `siphon/core/pipeline.py` | Config hash, RunTracker integration, resume support | Modify |
| `siphon/cli.py` | `--resume` and `--batch-size` flags | Modify |
| `tests/db/test_run_tracker.py` | Unit tests for RunTracker | Create |
| `tests/db/test_inserter_batched.py` | Tests for batched inserter | Create |
| `tests/core/test_pipeline_resume.py` | Pipeline tests for run tracking + resume | Create |
| `tests/test_integration_resume.py` | End-to-end test: failed run, then resume | Create |

The `_siphon_runs` table is a Siphon-managed bookkeeping table. It's auto-created when `track_runs: true` regardless of the user's `--create-tables` flag (because the user didn't define it — Siphon did).

---

## Trade-Offs to Document

**v2 atomicity:** entire batch committed in one transaction; failure rolls back everything.

**v3 with batched commits:** each batch (default 500 records) commits independently. A failure rolls back only the failing batch. Earlier batches remain in the DB. This is what makes resumability possible — there's something to resume from.

**Users who want all-or-nothing** can set `batch_size` larger than their dataset, or disable run tracking entirely with `track_runs: false`.

---

## Task Breakdown

### Task 1: Branch + baseline

**Files:** branch only

- [ ] **Step 1: Cut a new branch from `v3-phase-2-dry-run-diff`**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v3-phase-2-dry-run-diff
git pull
git checkout -b v3-phase-3-resumable-runs
```

- [ ] **Step 2: Verify the v3.2 baseline**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `924 passed`.

- [ ] **Step 3: No commit yet** — branch creation alone doesn't need a commit.

---

### Task 2: PipelineConfig — add batch_size and track_runs

**Files:**
- Modify: `siphon/config/schema.py`
- Modify: `tests/config/test_schema.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/config/test_schema.py`:

```python
class TestPipelineConfigBatchAndTracking:
    def test_default_batch_size(self):
        from siphon.config.schema import PipelineConfig
        cfg = PipelineConfig()
        assert cfg.batch_size == 500

    def test_default_track_runs(self):
        from siphon.config.schema import PipelineConfig
        cfg = PipelineConfig()
        assert cfg.track_runs is True

    def test_custom_batch_size(self):
        from siphon.config.schema import PipelineConfig
        cfg = PipelineConfig(batch_size=10)
        assert cfg.batch_size == 10

    def test_track_runs_disabled(self):
        from siphon.config.schema import PipelineConfig
        cfg = PipelineConfig(track_runs=False)
        assert cfg.track_runs is False

    def test_zero_batch_size_rejected(self):
        from pydantic import ValidationError
        from siphon.config.schema import PipelineConfig
        with pytest.raises(ValidationError):
            PipelineConfig(batch_size=0)

    def test_negative_batch_size_rejected(self):
        from pydantic import ValidationError
        from siphon.config.schema import PipelineConfig
        with pytest.raises(ValidationError):
            PipelineConfig(batch_size=-1)
```

- [ ] **Step 2: Verify the tests fail**

```bash
.venv/bin/pytest tests/config/test_schema.py::TestPipelineConfigBatchAndTracking -v
```

Expected: FAIL on attribute access (`batch_size` and `track_runs` don't exist).

- [ ] **Step 3: Update `PipelineConfig`**

In `siphon/config/schema.py`, find the `PipelineConfig` class and add two new fields. Keep existing fields unchanged. The class should look like:

```python
class PipelineConfig(BaseModel):
    """Runtime pipeline options."""

    model_config = ConfigDict(populate_by_name=True)

    chunk_size: int = 25
    review: bool = False
    log_level: Literal["debug", "info", "warning", "error"] = "info"
    log_dir: str | None = None
    batch_size: int = Field(default=500, gt=0)
    track_runs: bool = True
```

If the file doesn't already import `Field`, ensure `from pydantic import BaseModel, ConfigDict, Field, model_validator` includes `Field`.

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/config/test_schema.py::TestPipelineConfigBatchAndTracking -v
```

Expected: 6 passed.

- [ ] **Step 5: Run full schema suite for regressions**

```bash
.venv/bin/pytest tests/config/ -q
```

Expected: All config tests pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/config/schema.py tests/config/test_schema.py
git commit -m "feat: add batch_size and track_runs to PipelineConfig"
```

---

### Task 3: RunTracker skeleton + table model

**Files:**
- Create: `siphon/db/run_tracker.py`
- Create: `tests/db/test_run_tracker.py`

The `_siphon_runs` table is a separate ORM model — not part of the user's `ModelGenerator` output. It uses its own declarative base so it can be created independently.

- [ ] **Step 1: Write the failing test**

Create `tests/db/test_run_tracker.py`:

```python
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/db/test_run_tracker.py -v
```

Expected: ImportError on `siphon.db.run_tracker`.

- [ ] **Step 3: Create the module**

Create `siphon/db/run_tracker.py`:

```python
"""Manages the _siphon_runs metadata table for run tracking and resumability."""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase

from siphon.db.engine import DatabaseEngine

logger = logging.getLogger("siphon")


class _RunsBase(DeclarativeBase):
    """Dedicated declarative base for the _siphon_runs table.

    Kept separate from user data models so that creating the runs table
    doesn't accidentally create or drop user tables.
    """
    pass


class SiphonRun(_RunsBase):
    """A row in the _siphon_runs metadata table."""

    __tablename__ = "_siphon_runs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_name = Column(String(255), nullable=False)
    source_file = Column(String(500), nullable=False)
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    status = Column(String(20), nullable=False)  # running | completed | failed
    total_records = Column(Integer, nullable=False)
    processed_count = Column(Integer, nullable=False, default=0)
    error_message = Column(Text, nullable=True)
    config_hash = Column(String(64), nullable=False)


class RunTracker:
    """Manages the lifecycle of run records in _siphon_runs."""

    def __init__(self, db_engine: DatabaseEngine) -> None:
        self._db = db_engine

    async def create_runs_table(self) -> None:
        """Create the _siphon_runs table if it does not already exist."""
        async with self._db.engine.begin() as conn:
            await conn.run_sync(_RunsBase.metadata.create_all)
        logger.info("Verified _siphon_runs metadata table exists")
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/db/test_run_tracker.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/run_tracker.py tests/db/test_run_tracker.py
git commit -m "feat: RunTracker skeleton with _siphon_runs table model"
```

---

### Task 4: RunTracker — start_run, update_progress, complete_run, fail_run

**Files:**
- Modify: `siphon/db/run_tracker.py`
- Modify: `tests/db/test_run_tracker.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_run_tracker.py`:

```python
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/db/test_run_tracker.py::TestRunLifecycle -v
```

Expected: FAIL on missing methods.

- [ ] **Step 3: Implement the lifecycle methods**

Add these methods to the `RunTracker` class in `siphon/db/run_tracker.py`:

```python
    async def start_run(
        self,
        pipeline_name: str,
        source_file: str,
        total_records: int,
        config_hash: str,
    ) -> int:
        """Insert a new 'running' row and return its id."""
        async with self._db.session() as session:
            run = SiphonRun(
                pipeline_name=pipeline_name,
                source_file=source_file,
                started_at=datetime.now(timezone.utc),
                status="running",
                total_records=total_records,
                processed_count=0,
                config_hash=config_hash,
            )
            session.add(run)
            await session.commit()
            await session.refresh(run)
            return run.id

    async def update_progress(self, run_id: int, processed_count: int) -> None:
        """Update the processed_count for a run."""
        from sqlalchemy import update

        async with self._db.session() as session:
            await session.execute(
                update(SiphonRun)
                .where(SiphonRun.id == run_id)
                .values(processed_count=processed_count)
            )
            await session.commit()

    async def complete_run(self, run_id: int) -> None:
        """Mark a run as completed."""
        from sqlalchemy import update

        async with self._db.session() as session:
            await session.execute(
                update(SiphonRun)
                .where(SiphonRun.id == run_id)
                .values(
                    status="completed",
                    completed_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()

    async def fail_run(self, run_id: int, error_message: str) -> None:
        """Mark a run as failed with an error message."""
        from sqlalchemy import update

        async with self._db.session() as session:
            await session.execute(
                update(SiphonRun)
                .where(SiphonRun.id == run_id)
                .values(
                    status="failed",
                    error_message=error_message,
                    completed_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()

    async def get_run(self, run_id: int) -> SiphonRun | None:
        """Fetch a run by id."""
        from sqlalchemy import select

        async with self._db.session() as session:
            result = await session.execute(
                select(SiphonRun).where(SiphonRun.id == run_id)
            )
            return result.scalar_one_or_none()
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/db/test_run_tracker.py -v
```

Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/run_tracker.py tests/db/test_run_tracker.py
git commit -m "feat: RunTracker lifecycle — start, update, complete, fail"
```

---

### Task 5: RunTracker — find_resumable_run

**Files:**
- Modify: `siphon/db/run_tracker.py`
- Modify: `tests/db/test_run_tracker.py`

A "resumable run" is the most recent `failed` run for the same pipeline+source+config combination. Different config (different hash) means we can't safely resume — the data shape may have changed.

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_run_tracker.py`:

```python
class TestFindResumableRun:
    async def test_no_failed_run_returns_none(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        result = await tracker.find_resumable_run("p", "/f.csv", "h")
        assert result is None

    async def test_completed_run_is_not_resumable(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f.csv", 10, "h")
        await tracker.complete_run(run_id)
        result = await tracker.find_resumable_run("p", "/f.csv", "h")
        assert result is None

    async def test_failed_run_is_resumable(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f.csv", 10, "h")
        await tracker.update_progress(run_id, 4)
        await tracker.fail_run(run_id, "boom")

        result = await tracker.find_resumable_run("p", "/f.csv", "h")
        assert result is not None
        assert result.id == run_id
        assert result.processed_count == 4

    async def test_different_pipeline_name_not_resumable(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p1", "/f.csv", 10, "h")
        await tracker.fail_run(run_id, "boom")

        result = await tracker.find_resumable_run("p2", "/f.csv", "h")
        assert result is None

    async def test_different_source_file_not_resumable(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f1.csv", 10, "h")
        await tracker.fail_run(run_id, "boom")

        result = await tracker.find_resumable_run("p", "/f2.csv", "h")
        assert result is None

    async def test_different_config_hash_not_resumable(self, engine_setup):
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()
        run_id = await tracker.start_run("p", "/f.csv", 10, "h1")
        await tracker.fail_run(run_id, "boom")

        result = await tracker.find_resumable_run("p", "/f.csv", "h2")
        assert result is None

    async def test_returns_most_recent_failed_run(self, engine_setup):
        """If multiple failed runs exist, return the most recent."""
        tracker = RunTracker(engine_setup)
        await tracker.create_runs_table()

        first_id = await tracker.start_run("p", "/f.csv", 10, "h")
        await tracker.update_progress(first_id, 2)
        await tracker.fail_run(first_id, "first")

        second_id = await tracker.start_run("p", "/f.csv", 10, "h")
        await tracker.update_progress(second_id, 5)
        await tracker.fail_run(second_id, "second")

        result = await tracker.find_resumable_run("p", "/f.csv", "h")
        assert result.id == second_id
        assert result.processed_count == 5
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/db/test_run_tracker.py::TestFindResumableRun -v
```

Expected: FAIL on missing method.

- [ ] **Step 3: Implement `find_resumable_run`**

Add this method to `RunTracker`:

```python
    async def find_resumable_run(
        self,
        pipeline_name: str,
        source_file: str,
        config_hash: str,
    ) -> SiphonRun | None:
        """Return the most recent failed run for this pipeline+source+config, or None."""
        from sqlalchemy import select

        async with self._db.session() as session:
            result = await session.execute(
                select(SiphonRun)
                .where(SiphonRun.pipeline_name == pipeline_name)
                .where(SiphonRun.source_file == source_file)
                .where(SiphonRun.config_hash == config_hash)
                .where(SiphonRun.status == "failed")
                .order_by(SiphonRun.id.desc())
                .limit(1)
            )
            return result.scalar_one_or_none()
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/db/test_run_tracker.py -v
```

Expected: 15 passed (8 existing + 7 new).

- [ ] **Step 5: Commit**

```bash
git add siphon/db/run_tracker.py tests/db/test_run_tracker.py
git commit -m "feat: RunTracker.find_resumable_run for most recent failed run"
```

---

### Task 6: Inserter — batched commits with progress callback

**Files:**
- Modify: `siphon/db/inserter.py`
- Create: `tests/db/test_inserter_batched.py`

The current `Inserter.insert()` commits one transaction containing all records. This task adds batching: records are split into groups of `batch_size`, each group committed in its own transaction. After each successful batch, an optional progress callback is invoked.

When `batch_size=None` (or larger than the record count), behavior is identical to v3.2 — one transaction, no callback overhead.

- [ ] **Step 1: Write the failing tests**

Create `tests/db/test_inserter_batched.py`:

```python
"""Tests for batched insertion with progress callback."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator


def _make_config() -> SiphonConfig:
    return SiphonConfig.model_validate({
        "name": "batched-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "items", "column": "name"}},
            ],
            "tables": {"items": {"primary_key": {"column": "id", "type": "auto_increment"}}},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def setup():
    config = _make_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestBatchedInsert:
    async def test_batch_size_none_inserts_all_at_once(self, setup):
        """Default batch_size=None preserves v2 behavior."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []

        def on_batch(committed_count: int) -> None:
            progress_calls.append(committed_count)

        records = [{"name": f"r{i}"} for i in range(5)]
        n = await inserter.insert(records, batch_size=None, on_batch=on_batch)

        assert n == 5
        # Single batch → one progress callback for the whole insert
        assert progress_calls == [5]

    async def test_batch_size_2_invokes_callback_per_batch(self, setup):
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []

        def on_batch(committed_count: int) -> None:
            progress_calls.append(committed_count)

        records = [{"name": f"r{i}"} for i in range(5)]
        n = await inserter.insert(records, batch_size=2, on_batch=on_batch)

        assert n == 5
        # 5 records, batch_size=2 → batches of [2, 2, 1] → cumulative [2, 4, 5]
        assert progress_calls == [2, 4, 5]

    async def test_batch_size_larger_than_records_one_batch(self, setup):
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []
        records = [{"name": f"r{i}"} for i in range(3)]
        await inserter.insert(records, batch_size=100,
                              on_batch=lambda n: progress_calls.append(n))

        assert progress_calls == [3]

    async def test_no_callback_does_not_error(self, setup):
        """on_batch is optional."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)
        n = await inserter.insert([{"name": "x"}, {"name": "y"}], batch_size=1)
        assert n == 2

    async def test_batch_failure_rolls_back_only_failing_batch(self, setup):
        """Earlier successfully-committed batches are NOT rolled back."""
        from sqlalchemy import UniqueConstraint
        from siphon.utils.errors import DatabaseError

        config, engine, model_gen = setup
        # Add a UNIQUE constraint so a duplicate fails the second batch
        items = model_gen.models["items"].__table__
        items.append_constraint(UniqueConstraint("name", name="uq_items_name"))
        # Recreate the table with the new constraint
        await engine.create_tables(model_gen.base)

        inserter = Inserter(config, engine, model_gen)

        # First batch: r0, r1 (succeed). Second batch: r1 (duplicate!), r2 (won't reach).
        records = [
            {"name": "r0"},
            {"name": "r1"},
            {"name": "r1"},   # duplicate inside the second batch
            {"name": "r2"},
        ]

        progress_calls = []
        with pytest.raises(DatabaseError):
            await inserter.insert(records, batch_size=2,
                                  on_batch=lambda n: progress_calls.append(n))

        # First batch committed successfully; second batch rolled back.
        async with engine.session() as session:
            existing = await session.execute(select(model_gen.models["items"].name))
            names = sorted([n[0] for n in existing])
        assert names == ["r0", "r1"]
        # Progress was reported once (after the first successful batch)
        assert progress_calls == [2]
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/db/test_inserter_batched.py -v
```

Expected: FAIL — `Inserter.insert` doesn't accept `batch_size` or `on_batch` yet.

- [ ] **Step 3: Modify the Inserter**

In `siphon/db/inserter.py`, the existing `insert()` method has a single transaction wrapping all records. Refactor it so the per-record loop is invocable on a batch slice and is wrapped in its own transaction.

Read the current `siphon/db/inserter.py` to understand the existing structure first. The current method body looks like:

```python
async def insert(self, records: list[dict], *, target_tables: set[str] | None = None) -> int:
    # ... topological sort and table_fields setup ...
    inserted_count = 0
    async with self._db.session() as session:
        try:
            async with session.begin():
                for record in records:
                    # ... insert logic with FK resolution and junctions ...
                    inserted_count += 1
        except Exception as e:
            raise DatabaseError(...) from e
    return inserted_count
```

Refactor by extracting the inner per-record loop into a helper, and have `insert()` chunk the records and call the helper per chunk. The new structure:

```python
async def insert(
    self,
    records: list[dict],
    *,
    target_tables: set[str] | None = None,
    batch_size: int | None = None,
    on_batch=None,
) -> int:
    """Insert records, optionally in batches with a per-batch progress callback.

    Args:
        records: List of mapped record dicts.
        target_tables: If provided, only insert into these tables.
        batch_size: If set, commit every N records in a separate transaction.
                    If None, all records committed in one transaction (v2 behavior).
        on_batch: Optional callable invoked after each successful batch commit
                  with the cumulative committed count.

    Returns total number of records inserted across all batches.
    Raises DatabaseError on failure (the failing batch is rolled back).
    """
    # Existing setup logic stays here:
    table_order = self.topological_sort()
    if target_tables is not None:
        table_order = [t for t in table_order if t in target_tables]

    # Sort records for self-referential relationships
    for rel in self._config.relationships:
        if isinstance(rel, BelongsToRelationship) and rel.table == rel.references:
            records = self._sort_records_for_self_ref(records, rel)
            break

    # Group fields by table — same as before
    table_fields: dict[str, list] = defaultdict(list)
    for field in self._config.schema_.fields:
        table_fields[field.db.table].append(field)
    if self._config.schema_.collections:
        for collection in self._config.schema_.collections:
            for field in collection.fields:
                existing = table_fields[field.db.table]
                if not any(f.db.column == field.db.column for f in existing):
                    table_fields[field.db.table].append(field)

    junctions = [
        r for r in self._config.relationships if isinstance(r, JunctionRelationship)
    ]
    belongs_tos = [
        r for r in self._config.relationships if isinstance(r, BelongsToRelationship)
    ]

    # Determine batch size
    effective_batch = batch_size if batch_size and batch_size > 0 else len(records) or 1

    inserted_count = 0
    for batch_start in range(0, len(records), effective_batch):
        batch = records[batch_start : batch_start + effective_batch]
        try:
            async with self._db.session() as session:
                async with session.begin():
                    for record in batch:
                        await self._insert_one_record(
                            session, record, table_order, table_fields,
                            junctions, belongs_tos,
                        )
        except Exception as e:
            raise DatabaseError(f"Batch insert failed at record {inserted_count + 1}: {e}") from e

        inserted_count += len(batch)
        if on_batch is not None:
            on_batch(inserted_count)

    return inserted_count
```

Then extract the existing per-record body into a method named `_insert_one_record`:

```python
async def _insert_one_record(
    self,
    session,
    record: dict,
    table_order: list[str],
    table_fields: dict,
    junctions: list,
    belongs_tos: list,
) -> None:
    """Insert a single record across all relevant tables and junctions."""
    record_ids: dict[str, any] = {}

    for table_name in table_order:
        model = self._models[table_name]
        pk_config = self._config.schema_.tables[table_name].primary_key

        # ... copy the existing per-table inner loop verbatim from the old insert() ...
        # Build row data, resolve FKs, generate UUIDs, insert (or upsert), capture PK.
        # Update record_ids[table_name] = pk_value
        # Update self._lookup_cache for this table

    # Insert junction rows for this record
    for junc in junctions:
        t1, t2 = junc.link
        if t1 in record_ids and t2 in record_ids:
            junc_model = self._models[junc.through]
            junc_row = junc_model(**{
                junc.columns[t1]: record_ids[t1],
                junc.columns[t2]: record_ids[t2],
            })
            session.add(junc_row)
```

**IMPORTANT:** The exact body of `_insert_one_record` must match the current per-record body inside `insert()` — including the upsert dispatch, the FK resolution via `_execute_upsert`, lookup cache updates, and junction creation. Read the current code carefully and keep all that logic intact.

After refactoring, the public `insert()` signature has new optional parameters but its behavior with `batch_size=None` is identical to before.

- [ ] **Step 4: Run the new tests**

```bash
.venv/bin/pytest tests/db/test_inserter_batched.py -v
```

Expected: 5 passed.

- [ ] **Step 5: Run all existing inserter and DB tests for regressions**

```bash
.venv/bin/pytest tests/db/ -q
```

Expected: All tests pass.

- [ ] **Step 6: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All tests pass.

- [ ] **Step 7: Commit**

```bash
git add siphon/db/inserter.py tests/db/test_inserter_batched.py
git commit -m "feat: Inserter — batched commits with progress callback"
```

---

### Task 7: Pipeline — compute config hash + RunTracker integration (no resume yet)

**Files:**
- Modify: `siphon/core/pipeline.py`
- Create: `tests/core/test_pipeline_resume.py`

This task wires the RunTracker into the pipeline: every non-dry-run insertion creates a run record, updates progress per batch, and finalizes the run as completed or failed. Resume support comes in the next task.

- [ ] **Step 1: Write the failing tests**

Create `tests/core/test_pipeline_resume.py`:

```python
"""Pipeline tests for run tracking and resume."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _config_yaml(tmp_path: Path, batch_size: int = 500,
                 track_runs: bool = True) -> Path:
    """Write a config file with batched commits / run tracking enabled."""
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
    from sqlalchemy import text
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
        # Now do a dry run — should not add another _siphon_runs row
        before = await _query(config.database.url,
                              "SELECT COUNT(*) FROM _siphon_runs")
        await Pipeline(config).run(csv_path, dry_run=True, no_review=True)
        after = await _query(config.database.url,
                             "SELECT COUNT(*) FROM _siphon_runs")

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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/core/test_pipeline_resume.py -v
```

Expected: FAIL — `_siphon_runs` table not created and run tracking not happening.

- [ ] **Step 3: Modify the pipeline**

In `siphon/core/pipeline.py`:

(a) Add imports at the top:

```python
import hashlib
import json
from siphon.db.run_tracker import RunTracker
```

(b) Add a private helper method to the `Pipeline` class for computing config hash:

```python
def _config_hash(self) -> str:
    """Stable SHA-256 hash of the config (excluding fields that don't affect data)."""
    payload = self._config.model_dump(by_alias=True, mode="json")
    # Remove fields that don't affect data — log_level, log_dir, etc.
    payload.get("pipeline", {}).pop("log_level", None)
    payload.get("pipeline", {}).pop("log_dir", None)
    serialized = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
```

(c) Find the section in `run()` that does insertion (after dry_run handling). It currently looks like:

```python
        # 4. Prepare DB components (needed for both review and insertion)
        db_engine = DatabaseEngine(self._config.database)
        try:
            model_gen = ModelGenerator(self._config)
            model_gen.generate()

            if create_tables:
                await db_engine.create_tables(model_gen.base)
            else:
                # ... verify_tables ...

            inserter = Inserter(self._config, db_engine, model_gen)
            await inserter.load_existing_keys()

            # 5. Review (human-in-the-loop)
            if not no_review and self._config.pipeline.review:
                # ...

            # 6. Insert
            result.total_inserted = await inserter.insert(valid_records)
        finally:
            await db_engine.dispose()
```

Replace the insertion section to track runs. Wrap the `inserter.insert(...)` call:

```python
            # 6. Insert (with run tracking)
            run_tracker = None
            run_id = None
            if self._config.pipeline.track_runs:
                run_tracker = RunTracker(db_engine)
                await run_tracker.create_runs_table()
                run_id = await run_tracker.start_run(
                    pipeline_name=self._config.name,
                    source_file=str(input_path),
                    total_records=len(valid_records),
                    config_hash=self._config_hash(),
                )

            async def _on_batch(committed: int) -> None:
                if run_tracker is not None and run_id is not None:
                    await run_tracker.update_progress(run_id, committed)

            try:
                result.total_inserted = await inserter.insert(
                    valid_records,
                    batch_size=self._config.pipeline.batch_size,
                    on_batch=lambda n: None,  # synchronous wrapper, see note below
                )
            except Exception as e:
                if run_tracker is not None and run_id is not None:
                    await run_tracker.fail_run(run_id, str(e))
                raise

            if run_tracker is not None and run_id is not None:
                await run_tracker.complete_run(run_id)
```

**Synchronous callback note:** `Inserter.insert` calls `on_batch(committed)` synchronously per the Task 6 design. To call the async `update_progress`, we need an async wrapper. The simplest approach is to change the callback signature in Task 6 to support both sync and async callbacks. But to keep this plan correct without re-doing Task 6, do this instead:

Replace the `on_batch=lambda n: None` placeholder with a synchronous closure that schedules an awaitable progress update for after the loop. But that's complex. The cleanest fix is to make the Inserter call the callback using `await` if it's a coroutine. Update Task 6's design as follows:

In the inserter's batch loop (`siphon/db/inserter.py`), replace:

```python
        if on_batch is not None:
            on_batch(inserted_count)
```

with:

```python
        if on_batch is not None:
            res = on_batch(inserted_count)
            # Support both sync and async callbacks
            if asyncio.iscoroutine(res):
                await res
```

Add `import asyncio` at the top of `siphon/db/inserter.py` if it's not already imported.

Then in the pipeline, the callback can be async:

```python
            async def _on_batch(committed: int) -> None:
                if run_tracker is not None and run_id is not None:
                    await run_tracker.update_progress(run_id, committed)

            try:
                result.total_inserted = await inserter.insert(
                    valid_records,
                    batch_size=self._config.pipeline.batch_size,
                    on_batch=_on_batch,
                )
            except Exception as e:
                if run_tracker is not None and run_id is not None:
                    await run_tracker.fail_run(run_id, str(e))
                raise

            if run_tracker is not None and run_id is not None:
                await run_tracker.complete_run(run_id)
```

If the Inserter callback support change wasn't included in Task 6, make it part of this task and update the existing inserter tests if any of them used async callbacks (none of the Task 6 tests did — they used sync `lambda` callbacks, which still work).

- [ ] **Step 4: Run the new tests**

```bash
.venv/bin/pytest tests/core/test_pipeline_resume.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/core/pipeline.py siphon/db/inserter.py tests/core/test_pipeline_resume.py
git commit -m "feat: pipeline tracks runs in _siphon_runs with progress callbacks"
```

---

### Task 8: Pipeline — resume support

**Files:**
- Modify: `siphon/core/pipeline.py`
- Modify: `tests/core/test_pipeline_resume.py`

When `resume=True` is passed to `Pipeline.run()`, the pipeline finds the most recent failed run for the same pipeline+source+config and skips records up to that run's `processed_count`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/core/test_pipeline_resume.py`:

```python
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
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/core/test_pipeline_resume.py::TestResume -v
```

Expected: FAIL — `Pipeline.run()` doesn't accept `resume`.

- [ ] **Step 3: Add resume support to `Pipeline.run()`**

In `siphon/core/pipeline.py`, find the `run()` method signature and add the `resume` parameter:

```python
async def run(
    self,
    input_path: str | Path,
    *,
    dry_run: bool = False,
    no_review: bool = False,
    create_tables: bool = False,
    sheet: str | int | None = None,
    resume: bool = False,
) -> PipelineResult:
```

Then in the run-tracking block (added in Task 7), before calling `start_run`, check for a resumable run and slice the records accordingly. Replace the start_run section with:

```python
            # 6. Insert (with run tracking + optional resume)
            run_tracker = None
            run_id = None
            records_to_insert = valid_records

            if self._config.pipeline.track_runs:
                run_tracker = RunTracker(db_engine)
                await run_tracker.create_runs_table()

                if resume:
                    resumable = await run_tracker.find_resumable_run(
                        pipeline_name=self._config.name,
                        source_file=str(input_path),
                        config_hash=self._config_hash(),
                    )
                    if resumable is not None:
                        skip_n = resumable.processed_count
                        logger.info(
                            "Resuming from run id=%s — skipping first %d records",
                            resumable.id, skip_n,
                        )
                        records_to_insert = valid_records[skip_n:]
                    else:
                        logger.info("No resumable run found — running from start")

                run_id = await run_tracker.start_run(
                    pipeline_name=self._config.name,
                    source_file=str(input_path),
                    total_records=len(valid_records),
                    config_hash=self._config_hash(),
                )

            async def _on_batch(committed: int) -> None:
                if run_tracker is not None and run_id is not None:
                    # processed_count is relative to the original valid_records,
                    # so add the number of skipped records.
                    skipped = len(valid_records) - len(records_to_insert)
                    await run_tracker.update_progress(run_id, skipped + committed)

            try:
                result.total_inserted = await inserter.insert(
                    records_to_insert,
                    batch_size=self._config.pipeline.batch_size,
                    on_batch=_on_batch,
                )
            except Exception as e:
                if run_tracker is not None and run_id is not None:
                    await run_tracker.fail_run(run_id, str(e))
                raise

            if run_tracker is not None and run_id is not None:
                await run_tracker.complete_run(run_id)
```

- [ ] **Step 4: Run the new tests**

```bash
.venv/bin/pytest tests/core/test_pipeline_resume.py::TestResume -v
```

Expected: 2 passed.

- [ ] **Step 5: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/core/pipeline.py tests/core/test_pipeline_resume.py
git commit -m "feat: pipeline supports --resume to skip already-processed records"
```

---

### Task 9: CLI — --resume and --batch-size flags

**Files:**
- Modify: `siphon/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cli.py`:

```python
class TestResumeAndBatchSizeFlags:
    def test_resume_flag_passed_to_pipeline(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_load.return_value = MagicMock()
            mock_load.return_value.pipeline.log_level = "info"
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv",
                "--config", str(config_file),
                "--no-review",
                "--resume",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs["resume"] is True

    def test_batch_size_flag_overrides_config(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.pipeline.batch_size = 500  # original config value
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv",
                "--config", str(config_file),
                "--no-review",
                "--batch-size", "10",
            ])

        # The CLI overwrites the config's batch_size before constructing Pipeline
        assert mock_cfg.pipeline.batch_size == 10

    def test_resume_default_is_false(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_load.return_value = MagicMock()
            mock_load.return_value.pipeline.log_level = "info"
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv", "--config", str(config_file), "--no-review",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs.get("resume", False) is False
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_cli.py::TestResumeAndBatchSizeFlags -v
```

Expected: FAIL — flags don't exist.

- [ ] **Step 3: Add flags to the `run` command**

In `siphon/cli.py`, find the `run` command and add two new options to the function signature (place them next to the other flags). The relevant additions:

```python
    resume: bool = typer.Option(False, "--resume", help="Continue from the last failed run"),
    batch_size: Optional[int] = typer.Option(None, "--batch-size", help="Records per commit (overrides config)"),
```

Inside the `run` function, after `cfg = load_config(config)` and the verbose/quiet handling but before constructing the `Pipeline`, apply the batch_size override:

```python
        if batch_size is not None:
            cfg.pipeline.batch_size = batch_size
```

And in the `pipeline.run(...)` call, add `resume=resume` to the kwargs:

```python
        result = asyncio.run(
            pipeline.run(
                input_path,
                dry_run=dry_run,
                no_review=no_review,
                create_tables=create_tables,
                sheet=sheet,
                resume=resume,
            )
        )
```

- [ ] **Step 4: Run the new tests**

```bash
.venv/bin/pytest tests/test_cli.py::TestResumeAndBatchSizeFlags -v
```

Expected: 3 passed.

- [ ] **Step 5: Update existing CLI tests that assert exact pipeline.run kwargs**

There may be a test like `test_run_passes_pipeline_args` that asserts the exact kwargs. Find it:

```bash
grep -n "assert_awaited_once_with\|run.assert_called" tests/test_cli.py
```

If such a test exists, update its assertion to include `resume=False` (the default). Read the existing assertion and add the new kwargs.

- [ ] **Step 6: Run all CLI tests**

```bash
.venv/bin/pytest tests/test_cli.py -v
```

Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add siphon/cli.py tests/test_cli.py
git commit -m "feat: --resume and --batch-size CLI flags"
```

---

### Task 10: End-to-end integration test

**Files:**
- Create: `tests/test_integration_resume.py`

Verify the full flow against a file-based SQLite DB: a real failure mid-import, then resume picks up where it left off.

- [ ] **Step 1: Write the test**

Create `tests/test_integration_resume.py`:

```python
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
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            return result.fetchall()
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
```

- [ ] **Step 2: Run the tests**

```bash
.venv/bin/pytest tests/test_integration_resume.py -v
```

Expected: 2 passed.

- [ ] **Step 3: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration_resume.py
git commit -m "test: end-to-end resume integration test"
```

---

### Task 11: README + version bump + push

**Files:**
- Modify: `README.md`
- Modify: `siphon/__init__.py`

- [ ] **Step 1: Add a "Resumable Runs" section to `README.md`**

Find the existing "## Dry-Run with Diff" section. Add the new section IMMEDIATELY AFTER it (before the next `##` heading).

Use literal triple backticks in the actual file:

```markdown
## Resumable Runs

When importing large datasets, a failure mid-import doesn't mean starting over. Siphon tracks each run in a `_siphon_runs` metadata table inside the target database, and `--resume` continues from where the last failure stopped.

```bash
# Initial run fails partway through (e.g., network blip, constraint violation)
siphon run big_data.csv

# Fix whatever caused the failure, then resume
siphon run big_data.csv --resume
```

**How it works:**
- Records are inserted in batches (default 500). Each batch commits in its own transaction.
- After each successful batch, Siphon updates `processed_count` in `_siphon_runs`.
- On failure, the failing batch is rolled back; earlier batches stay committed.
- `--resume` finds the most recent failed run for the same pipeline name + source file + config hash, and skips records up to its `processed_count`.

**Configuration:**
```yaml
pipeline:
  batch_size: 500       # records per transaction commit (default 500)
  track_runs: true      # enable _siphon_runs table (default true)
```

**Trade-offs:**
- Batch commits are not atomic across the whole import. If you need all-or-nothing semantics, set `batch_size` larger than your dataset.
- Resume requires upserts or schemas with no unique constraints — otherwise re-processing already-inserted records will fail. Combine with `on_conflict.action: update` for safe resumability.
- Run tracking adds a `_siphon_runs` table to the target database. Disable with `track_runs: false` if you don't want it.
```

- [ ] **Step 2: Bump version in `siphon/__init__.py`**

Change:

```python
__version__ = "0.3.0a2"
```

to:

```python
__version__ = "0.3.0a3"
```

- [ ] **Step 3: Run the full suite once more**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All pass.

- [ ] **Step 4: Audit for proprietary references**

```bash
grep -ri "workshield\|work.shield" --include="*.py" --include="*.yaml" --include="*.md" --include="*.toml" .
```

Expected: No matches in production code (any matches in plan files are command quotations and can be ignored).

- [ ] **Step 5: Commit and push**

```bash
git add README.md siphon/__init__.py
git commit -m "chore: bump version to 0.3.0a3 (Phase 3 complete)"
git push -u origin v3-phase-3-resumable-runs
```

---

## Verification

After all tasks complete:

1. All tests pass: `.venv/bin/pytest tests/ -q`
2. The branch `v3-phase-3-resumable-runs` is pushed to origin.
3. CLI smoke test: with run tracking enabled and a populated `_siphon_runs` table containing a failed run, `siphon run data.csv --resume` skips already-processed records.
4. CLI smoke test: `siphon run data.csv --batch-size 10` overrides the config's batch_size.

## Out of Scope

- **Cleanup of `_siphon_runs` rows**: completed/failed runs accumulate forever. A future phase can add a TTL or `--cleanup-runs` command.
- **Mid-batch resume**: resume granularity is per-batch. A failure mid-batch rolls back the entire batch; resume starts from the last successful batch.
- **Resume across different machines**: the run record lives in the database, not on local disk, so this works automatically — just connect from the new machine and `--resume`.
- **Resume detection of already-inserted rows**: relies on `processed_count` from the prior run, not on querying the data tables. If `_siphon_runs` is wiped, resume can't find anything.

## Self-Review Notes

**Spec coverage check:**
- ✅ `_siphon_runs` table with all 10 columns — Task 3
- ✅ Batched commits with progress callback — Task 6
- ✅ Run lifecycle (start/update/complete/fail) — Task 4
- ✅ `find_resumable_run` keyed on pipeline+source+config_hash — Task 5
- ✅ `--resume` flag — Task 9
- ✅ `--batch-size N` flag — Task 9
- ✅ `batch_size` config option (default 500) — Task 2
- ✅ `track_runs` config option (default true) — Task 2

**Type consistency check:**
- `RunTracker(db_engine)` — single argument signature in Tasks 3, 4, 5, 7, 8, 10.
- `start_run(pipeline_name, source_file, total_records, config_hash) → int` — same across Tasks 4, 7, 8, 10.
- `find_resumable_run(pipeline_name, source_file, config_hash) → SiphonRun | None` — same in Tasks 5, 8, 10.
- `Inserter.insert(records, *, target_tables, batch_size, on_batch)` — same across Tasks 6, 7.

**Placeholder scan:** None.

**One deviation noted in Task 7:** the `on_batch` callback in `Inserter.insert` (Task 6) was originally specified as synchronous, but the pipeline (Task 7) needs it to be async to call `update_progress`. Task 7 includes the fix in the inserter to support both sync and async callbacks (one extra `if asyncio.iscoroutine(res): await res` block).
