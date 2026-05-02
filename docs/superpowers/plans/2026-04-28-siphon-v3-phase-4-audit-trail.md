# Siphon v3 Phase 4: Audit Trail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record every record-level action (insert, update, skip) in a `_siphon_audit` table so users can answer "what got imported when, and by whom?"

**Architecture:** A new `AuditLogger` class buffers per-record audit entries (target table, PK, action, field-level changes, source row, run_id, timestamp, reviewed_by) and flushes them in the same batch transaction as the data inserts. The `Inserter` calls into the logger as it processes each record. Reuses Phase 1's `_compute_changes` logic for old→new diffs and Phase 3's `run_id` for foreign-key linking. CLI gains `--user NAME` to identify who's running the import.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.0 async, Pydantic 2.0, json (stdlib), pytest, aiosqlite (test).

**Spec:** `docs/superpowers/specs/2026-04-22-siphon-v3-roadmap.md` (Phase 4 section)

**Branch:** Cut `v3-phase-4-audit-trail` from `v3-phase-3-resumable-runs`.

---

## Schema

The `_siphon_audit` table:

```sql
CREATE TABLE _siphon_audit (
  id              INTEGER PRIMARY KEY,
  run_id          INTEGER NOT NULL,           -- FK to _siphon_runs.id (no constraint)
  target_table    VARCHAR(255) NOT NULL,
  target_pk       VARCHAR(255) NOT NULL,      -- the inserted/updated row's PK value (as string)
  action          VARCHAR(10) NOT NULL,       -- insert | update | skip
  field_changes   TEXT NULL,                  -- JSON: {"col": {"old": ..., "new": ...}}
  source_file     VARCHAR(500) NULL,
  source_row      INTEGER NULL,               -- 1-based row number in source file
  reviewed_by     VARCHAR(255) NULL,          -- from --user flag or system username
  created_at      DATETIME NOT NULL
);
```

The `run_id` is intentionally not a hard FK — keeps the schema simple and allows audit cleanup without cascade complications.

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/db/audit.py` | `_siphon_audit` table model + `AuditLogger` class | Create |
| `siphon/db/inserter.py` | Emit audit entries during insertion | Modify |
| `siphon/config/schema.py` | Add `audit: bool = True` to `PipelineConfig` | Modify |
| `siphon/core/pipeline.py` | Wire `AuditLogger` + `--user` into the run | Modify |
| `siphon/cli.py` | Add `--user NAME` flag | Modify |
| `tests/db/test_audit.py` | Unit tests for `AuditLogger` | Create |
| `tests/db/test_inserter_audit.py` | Inserter integration tests for audit emission | Create |
| `tests/core/test_pipeline_audit.py` | Pipeline tests for audit + user identification | Create |
| `tests/test_integration_audit.py` | End-to-end audit verification | Create |

The `AuditLogger` lives next to `RunTracker` in `siphon/db/`. Same pattern: Siphon-managed metadata table with its own declarative base, separate from user data models.

---

## Audit Entry Buffering Model

To keep the implementation simple and the inserter's transaction semantics clean:

1. The `Inserter` instantiates an `AuditLogger` at the start of `insert()` if a `run_id` is provided.
2. As each record is inserted (or skipped via upsert), the inserter appends an in-memory entry to the logger's buffer.
3. After each successful batch commit, the inserter calls `logger.flush()` to write the buffered entries to `_siphon_audit` in a separate transaction.
4. If a batch fails, the buffered audit entries for that batch are discarded (no audit row exists for rolled-back data).

This keeps audit writes outside the data transaction (so audit failures don't break the import), but ensures audit rows are only written for committed data.

---

## Task Breakdown

### Task 1: Branch + baseline

**Files:** branch only

- [ ] **Step 1: Cut a new branch from `v3-phase-3-resumable-runs`**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v3-phase-3-resumable-runs
git pull
git checkout -b v3-phase-4-audit-trail
```

- [ ] **Step 2: Verify the v3.3 baseline**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `961 passed`.

- [ ] **Step 3: No commit yet** — branch creation alone doesn't need a commit.

---

### Task 2: PipelineConfig — add `audit` flag

**Files:**
- Modify: `siphon/config/schema.py`
- Modify: `tests/config/test_schema.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/config/test_schema.py`:

```python
class TestPipelineConfigAudit:
    def test_default_audit_is_true(self):
        from siphon.config.schema import PipelineConfig
        cfg = PipelineConfig()
        assert cfg.audit is True

    def test_audit_can_be_disabled(self):
        from siphon.config.schema import PipelineConfig
        cfg = PipelineConfig(audit=False)
        assert cfg.audit is False
```

- [ ] **Step 2: Verify the tests fail**

Run: `.venv/bin/pytest tests/config/test_schema.py::TestPipelineConfigAudit -v`
Expected: FAIL on attribute access.

- [ ] **Step 3: Update PipelineConfig**

In `siphon/config/schema.py`, find the `PipelineConfig` class. Add `audit: bool = True` after the existing `track_runs` field:

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
    audit: bool = True
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/pytest tests/config/test_schema.py::TestPipelineConfigAudit -v`
Expected: 2 passed.

- [ ] **Step 5: Run full schema suite**

Run: `.venv/bin/pytest tests/config/ -q`
Expected: All config tests pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/config/schema.py tests/config/test_schema.py
git commit -m "feat: add audit flag to PipelineConfig"
```

---

### Task 3: AuditLogger skeleton + table model

**Files:**
- Create: `siphon/db/audit.py`
- Create: `tests/db/test_audit.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/db/test_audit.py`:

```python
"""Tests for the AuditLogger — manages _siphon_audit metadata table."""

from __future__ import annotations

import pytest
from siphon.config.schema import DatabaseConfig
from siphon.db.audit import AuditEntry, AuditLogger, SiphonAudit
from siphon.db.engine import DatabaseEngine


@pytest.fixture
async def engine_setup():
    config = DatabaseConfig(url="sqlite+aiosqlite://")
    engine = DatabaseEngine(config)
    yield engine
    await engine.dispose()


class TestAuditLoggerSetup:
    async def test_audit_logger_can_be_constructed(self, engine_setup):
        logger = AuditLogger(engine_setup, run_id=1)
        assert logger is not None

    async def test_create_audit_table_creates_siphon_audit(self, engine_setup):
        from sqlalchemy import inspect

        logger = AuditLogger(engine_setup, run_id=1)
        await logger.create_audit_table()

        async with engine_setup.engine.connect() as conn:
            def _get_tables(sync_conn):
                return inspect(sync_conn).get_table_names()
            tables = await conn.run_sync(_get_tables)

        assert "_siphon_audit" in tables

    async def test_create_audit_table_is_idempotent(self, engine_setup):
        logger = AuditLogger(engine_setup, run_id=1)
        await logger.create_audit_table()
        await logger.create_audit_table()  # no error

    async def test_siphon_audit_model_columns(self):
        cols = {c.name for c in SiphonAudit.__table__.columns}
        assert cols == {
            "id", "run_id", "target_table", "target_pk", "action",
            "field_changes", "source_file", "source_row", "reviewed_by",
            "created_at",
        }

    async def test_audit_entry_dataclass_fields(self):
        entry = AuditEntry(
            target_table="companies",
            target_pk="42",
            action="insert",
        )
        assert entry.target_table == "companies"
        assert entry.target_pk == "42"
        assert entry.action == "insert"
        assert entry.field_changes is None
        assert entry.source_row is None
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/db/test_audit.py -v`
Expected: ImportError on `siphon.db.audit`.

- [ ] **Step 3: Create the module**

Create `siphon/db/audit.py`:

```python
"""Manages the _siphon_audit metadata table for per-record action logging."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

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


class _AuditBase(DeclarativeBase):
    """Dedicated declarative base for the _siphon_audit table.

    Kept separate from user data models and from _RunsBase in run_tracker.py
    so that creating the audit table doesn't accidentally touch other tables.
    """
    pass


class SiphonAudit(_AuditBase):
    """A row in the _siphon_audit metadata table."""

    __tablename__ = "_siphon_audit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, nullable=False)
    target_table = Column(String(255), nullable=False)
    target_pk = Column(String(255), nullable=False)
    action = Column(String(10), nullable=False)        # insert | update | skip
    field_changes = Column(Text, nullable=True)         # JSON-serialized dict
    source_file = Column(String(500), nullable=True)
    source_row = Column(Integer, nullable=True)
    reviewed_by = Column(String(255), nullable=True)
    created_at = Column(DateTime, nullable=False)


@dataclass
class AuditEntry:
    """In-memory audit entry awaiting flush."""
    target_table: str
    target_pk: str
    action: str                                # insert | update | skip
    field_changes: dict | None = None
    source_file: str | None = None
    source_row: int | None = None
    reviewed_by: str | None = None


class AuditLogger:
    """Buffers and flushes per-record audit entries to _siphon_audit.

    Entries are buffered in memory until flush() is called. Typical usage:
    the Inserter appends an entry per record processed in a batch, then
    calls flush() after the batch successfully commits.
    """

    def __init__(self, db_engine: DatabaseEngine, run_id: int) -> None:
        self._db = db_engine
        self._run_id = run_id
        self._buffer: list[AuditEntry] = []

    async def create_audit_table(self) -> None:
        """Create the _siphon_audit table if it does not already exist."""
        async with self._db.engine.begin() as conn:
            await conn.run_sync(_AuditBase.metadata.create_all)
        logger.info("Verified _siphon_audit metadata table exists")
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/pytest tests/db/test_audit.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/audit.py tests/db/test_audit.py
git commit -m "feat: AuditLogger skeleton with _siphon_audit table model"
```

---

### Task 4: AuditLogger — buffer, flush, clear

**Files:**
- Modify: `siphon/db/audit.py`
- Modify: `tests/db/test_audit.py`

- [ ] **Step 1: Write the failing tests**

Append to `tests/db/test_audit.py`:

```python
class TestAuditLoggerBuffer:
    async def test_record_adds_to_buffer(self, engine_setup):
        logger = AuditLogger(engine_setup, run_id=1)
        logger.record(AuditEntry(
            target_table="companies",
            target_pk="42",
            action="insert",
        ))
        assert len(logger.buffer) == 1
        assert logger.buffer[0].target_table == "companies"

    async def test_clear_empties_buffer(self, engine_setup):
        logger = AuditLogger(engine_setup, run_id=1)
        logger.record(AuditEntry(target_table="t", target_pk="1", action="insert"))
        logger.record(AuditEntry(target_table="t", target_pk="2", action="insert"))
        logger.clear()
        assert logger.buffer == []

    async def test_flush_writes_buffered_entries_and_clears_buffer(self, engine_setup):
        from sqlalchemy import select

        logger = AuditLogger(engine_setup, run_id=99)
        await logger.create_audit_table()

        logger.record(AuditEntry(target_table="companies", target_pk="42",
                                 action="insert"))
        logger.record(AuditEntry(target_table="addresses", target_pk="7",
                                 action="update",
                                 field_changes={"phone": {"old": "1", "new": "2"}}))

        await logger.flush()

        # Buffer cleared
        assert logger.buffer == []

        # DB has two rows
        async with engine_setup.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        assert len(rows) == 2
        assert {r.target_table for r in rows} == {"companies", "addresses"}

        update_row = next(r for r in rows if r.action == "update")
        # field_changes is stored as JSON
        import json
        assert json.loads(update_row.field_changes) == {
            "phone": {"old": "1", "new": "2"}
        }
        # run_id propagated
        assert all(r.run_id == 99 for r in rows)
        # created_at populated
        assert all(r.created_at is not None for r in rows)

    async def test_flush_with_empty_buffer_is_noop(self, engine_setup):
        logger = AuditLogger(engine_setup, run_id=1)
        await logger.create_audit_table()
        # No entries — flush should not error
        await logger.flush()

    async def test_flush_serializes_complex_field_changes(self, engine_setup):
        from sqlalchemy import select
        import json

        logger = AuditLogger(engine_setup, run_id=1)
        await logger.create_audit_table()

        logger.record(AuditEntry(
            target_table="t", target_pk="1", action="update",
            field_changes={
                "count": {"old": 5, "new": 10},
                "name": {"old": "OLD", "new": "NEW"},
                "active": {"old": True, "new": False},
            },
        ))
        await logger.flush()

        async with engine_setup.session() as session:
            row = (await session.execute(select(SiphonAudit))).scalar_one()
        parsed = json.loads(row.field_changes)
        assert parsed["count"] == {"old": 5, "new": 10}
        assert parsed["active"] == {"old": True, "new": False}

    async def test_record_propagates_optional_fields(self, engine_setup):
        from sqlalchemy import select

        logger = AuditLogger(engine_setup, run_id=1)
        await logger.create_audit_table()
        logger.record(AuditEntry(
            target_table="t", target_pk="1", action="insert",
            source_file="/data/import.csv",
            source_row=5,
            reviewed_by="alice",
        ))
        await logger.flush()

        async with engine_setup.session() as session:
            row = (await session.execute(select(SiphonAudit))).scalar_one()
        assert row.source_file == "/data/import.csv"
        assert row.source_row == 5
        assert row.reviewed_by == "alice"
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/db/test_audit.py::TestAuditLoggerBuffer -v`
Expected: FAIL on missing methods.

- [ ] **Step 3: Implement**

Add to `AuditLogger` in `siphon/db/audit.py`:

```python
    @property
    def buffer(self) -> list[AuditEntry]:
        """Read-only view of pending entries. Tests inspect this."""
        return self._buffer

    def record(self, entry: AuditEntry) -> None:
        """Buffer an audit entry for later flush."""
        self._buffer.append(entry)

    def clear(self) -> None:
        """Discard all buffered entries without writing."""
        self._buffer.clear()

    async def flush(self) -> None:
        """Write all buffered entries to _siphon_audit in one transaction.

        Clears the buffer on success. On error, entries remain buffered
        so the caller can decide whether to retry or discard.
        """
        if not self._buffer:
            return

        import json
        now = datetime.now(timezone.utc)
        rows = [
            SiphonAudit(
                run_id=self._run_id,
                target_table=e.target_table,
                target_pk=e.target_pk,
                action=e.action,
                field_changes=(
                    json.dumps(e.field_changes, default=str)
                    if e.field_changes is not None else None
                ),
                source_file=e.source_file,
                source_row=e.source_row,
                reviewed_by=e.reviewed_by,
                created_at=now,
            )
            for e in self._buffer
        ]

        async with self._db.session() as session:
            session.add_all(rows)
            await session.commit()

        self._buffer.clear()
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/pytest tests/db/test_audit.py -v`
Expected: 11 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/audit.py tests/db/test_audit.py
git commit -m "feat: AuditLogger record, clear, and flush"
```

---

### Task 5: Inserter — emit audit entries on insert/update

**Files:**
- Modify: `siphon/db/inserter.py`
- Create: `tests/db/test_inserter_audit.py`

The `Inserter` needs to:
1. Accept an optional `audit_logger: AuditLogger | None` parameter on `insert()`.
2. After each record is inserted/updated, append an `AuditEntry` to the logger's buffer (for inserts: action="insert", no field_changes; for upserts that updated: action="update" with field_changes).
3. After each successful batch commit, call `audit_logger.flush()`.
4. On batch failure, call `audit_logger.clear()` to discard buffered entries.

To compute field_changes for upserts, the inserter needs to know the OLD values before the upsert. This means doing a SELECT before the upsert to capture the existing row, comparing values, and emitting the diff.

For simplicity in this phase: only emit `field_changes` when the action was an update (existing row matched). For pure inserts, `field_changes=None`. For skips, `field_changes=None` and action="skip".

- [ ] **Step 1: Write the failing tests**

Create `tests/db/test_inserter_audit.py`:

```python
"""Tests for audit emission during insertion."""

from __future__ import annotations

import json
import pytest
from sqlalchemy import UniqueConstraint, select

from siphon.config.schema import SiphonConfig
from siphon.db.audit import AuditLogger, SiphonAudit
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator


def _make_config(on_conflict: dict | None = None) -> SiphonConfig:
    table_cfg = {"primary_key": {"column": "id", "type": "auto_increment"}}
    if on_conflict is not None:
        table_cfg["on_conflict"] = on_conflict
    return SiphonConfig.model_validate({
        "name": "audit-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "items", "column": "name"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "items", "column": "phone"}},
            ],
            "tables": {"items": table_cfg},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def setup_basic():
    config = _make_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    audit = AuditLogger(engine, run_id=42)
    await audit.create_audit_table()
    yield config, engine, model_gen, audit
    await engine.dispose()


@pytest.fixture
async def setup_upsert():
    config = _make_config({"key": ["name"], "action": "update", "update_columns": "all"})
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    audit = AuditLogger(engine, run_id=42)
    await audit.create_audit_table()
    yield config, engine, model_gen, audit
    await engine.dispose()


class TestInsertAudit:
    async def test_insert_emits_insert_audit_entry(self, setup_basic):
        config, engine, model_gen, audit = setup_basic
        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([{"name": "a", "phone": "111"}], audit_logger=audit)

        async with engine.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        assert len(rows) == 1
        assert rows[0].action == "insert"
        assert rows[0].target_table == "items"
        assert rows[0].run_id == 42
        assert rows[0].field_changes is None

    async def test_insert_emits_one_audit_per_record(self, setup_basic):
        config, engine, model_gen, audit = setup_basic
        inserter = Inserter(config, engine, model_gen)
        records = [{"name": f"r{i}", "phone": str(i)} for i in range(3)]
        await inserter.insert(records, audit_logger=audit)

        async with engine.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        assert len(rows) == 3
        assert all(r.action == "insert" for r in rows)

    async def test_inserter_works_without_audit_logger(self, setup_basic):
        """audit_logger=None preserves v3.3 behavior."""
        config, engine, model_gen, _ = setup_basic
        inserter = Inserter(config, engine, model_gen)
        n = await inserter.insert([{"name": "x"}], audit_logger=None)
        assert n == 1

        async with engine.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        assert rows == []


class TestUpsertAudit:
    async def test_update_emits_update_audit_with_field_changes(self, setup_upsert):
        config, engine, model_gen, audit = setup_upsert
        inserter = Inserter(config, engine, model_gen)

        # First insert
        await inserter.insert([{"name": "a", "phone": "OLD"}], audit_logger=audit)
        # Then update
        await inserter.insert([{"name": "a", "phone": "NEW"}], audit_logger=audit)

        async with engine.session() as session:
            rows = (await session.execute(
                select(SiphonAudit).order_by(SiphonAudit.id)
            )).scalars().all()
        assert len(rows) == 2
        assert rows[0].action == "insert"
        assert rows[0].field_changes is None
        assert rows[1].action == "update"
        assert rows[1].field_changes is not None
        changes = json.loads(rows[1].field_changes)
        assert changes == {"phone": {"old": "OLD", "new": "NEW"}}

    async def test_upsert_no_change_does_not_emit_audit(self, setup_upsert):
        """If an upsert results in identical values, no audit entry."""
        config, engine, model_gen, audit = setup_upsert
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "a", "phone": "111"}], audit_logger=audit)
        # Same values — no real change
        await inserter.insert([{"name": "a", "phone": "111"}], audit_logger=audit)

        async with engine.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        # 1 insert + 0 update (no-op) = 1
        assert len(rows) == 1
        assert rows[0].action == "insert"


class TestBatchAuditFlushing:
    async def test_each_batch_flushes_audit(self, setup_basic):
        """Batches commit independently; audit entries are flushed per batch."""
        config, engine, model_gen, audit = setup_basic
        inserter = Inserter(config, engine, model_gen)
        records = [{"name": f"r{i}"} for i in range(5)]

        await inserter.insert(records, batch_size=2, audit_logger=audit)

        async with engine.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        assert len(rows) == 5

    async def test_failed_batch_discards_buffered_audit(self, setup_basic):
        """If a batch fails, no audit entries for that batch are written."""
        from siphon.utils.errors import DatabaseError

        config, engine, model_gen, audit = setup_basic
        # Add a unique constraint to force failure
        items = model_gen.models["items"].__table__
        items.append_constraint(UniqueConstraint("name", name="uq_items_name"))
        await engine.create_tables(model_gen.base)

        inserter = Inserter(config, engine, model_gen)
        records = [
            {"name": "r0"},  # succeeds (batch 1)
            {"name": "r1"},  # succeeds (batch 1)
            {"name": "r1"},  # FAILS (batch 2 — duplicate)
            {"name": "r2"},  # never reached
        ]

        with pytest.raises(DatabaseError):
            await inserter.insert(records, batch_size=2, audit_logger=audit)

        async with engine.session() as session:
            rows = (await session.execute(select(SiphonAudit))).scalars().all()
        # Only the first batch's audits (2 inserts), not the failed batch
        assert len(rows) == 2
        assert {r.target_pk for r in rows}  # has values
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/db/test_inserter_audit.py -v`
Expected: FAIL — `Inserter.insert` doesn't accept `audit_logger`.

- [ ] **Step 3: Modify the Inserter**

In `siphon/db/inserter.py`:

(a) Add to imports at the top:
```python
from siphon.db.audit import AuditEntry, AuditLogger
```

(b) Add `audit_logger` parameter to the `insert()` signature:

```python
async def insert(
    self,
    records: list[dict],
    *,
    target_tables: set[str] | None = None,
    batch_size: int | None = None,
    on_batch=None,
    audit_logger: "AuditLogger | None" = None,
) -> int:
```

(c) In the batch loop, after a successful batch commit, flush the audit logger. On failure, clear it. Find the existing batch loop:

```python
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
            from siphon.utils.errors import DatabaseError
            raise DatabaseError(...) from e

        inserted_count += len(batch)
        if on_batch is not None:
            ...
```

Modify the inner work to thread `audit_logger` down to `_insert_one_record`, and to flush/clear after the batch:

```python
    for batch_start in range(0, len(records), effective_batch):
        batch = records[batch_start : batch_start + effective_batch]
        try:
            async with self._db.session() as session:
                async with session.begin():
                    for record in batch:
                        await self._insert_one_record(
                            session, record, table_order, table_fields,
                            junctions, belongs_tos,
                            audit_logger=audit_logger,
                        )
        except Exception as e:
            # Discard buffered audit entries for the failed batch
            if audit_logger is not None:
                audit_logger.clear()
            from siphon.utils.errors import DatabaseError
            raise DatabaseError(
                f"Insert failed at record {inserted_count + 1}: {e}"
            ) from e

        # Flush audit entries from this successful batch
        if audit_logger is not None:
            await audit_logger.flush()

        inserted_count += len(batch)
        if on_batch is not None:
            res = on_batch(inserted_count)
            if asyncio.iscoroutine(res):
                await res
```

(d) Update the `_insert_one_record` method signature to accept `audit_logger`:

```python
async def _insert_one_record(
    self,
    session,
    record: dict,
    table_order: list[str],
    table_fields: dict,
    junctions: list,
    belongs_tos: list,
    audit_logger: "AuditLogger | None" = None,
) -> None:
```

(e) Inside `_insert_one_record`, in the per-table loop, after a successful insert/upsert, emit an audit entry. Find the current ORM-insert branch and the upsert branch:

```python
            table_cfg = self._config.schema_.tables[table_name]
            if table_cfg.on_conflict is None or table_cfg.on_conflict.action == "error":
                # ORM insert path
                instance = model(**row_data)
                session.add(instance)
                await session.flush()
                pk_value = getattr(instance, pk_config.column)
            else:
                # Upsert path
                pk_value = await self._execute_upsert(...)
```

Add audit emission. For the ORM-insert path (always an insert):

```python
            if table_cfg.on_conflict is None or table_cfg.on_conflict.action == "error":
                instance = model(**row_data)
                session.add(instance)
                await session.flush()
                pk_value = getattr(instance, pk_config.column)
                if audit_logger is not None:
                    audit_logger.record(AuditEntry(
                        target_table=table_name,
                        target_pk=str(pk_value),
                        action="insert",
                    ))
```

For the upsert path, modify `_execute_upsert` to also return what action occurred and any field changes. The current `_execute_upsert` signature returns `pk_value`. Update it to return `(pk_value, action, field_changes)` where:
- `action` is "insert" | "update" | "skip"
- `field_changes` is None for insert/skip, or a `{col: {old, new}}` dict for update

Find the current `_execute_upsert` method and modify it:

```python
async def _execute_upsert(
    self,
    session,
    model,
    table_name: str,
    row_data: dict,
    pk_config,
    on_conflict_cfg,
):
    """Execute an upsert and return (pk_value, action, field_changes)."""
    db_conflict_key = self._field_names_to_columns(on_conflict_cfg.key)

    if on_conflict_cfg.update_columns == "all":
        db_update_columns = "all"
    else:
        db_update_columns = self._field_names_to_columns(on_conflict_cfg.update_columns)

    # Look up existing row BEFORE upsert to compute field changes
    existing_row = await self._lookup_existing_row(
        session, model, db_conflict_key, row_data
    )

    stmt = build_upsert_statement(
        dialect=self._dialect,
        table=model.__table__,
        row=row_data,
        conflict_key=db_conflict_key,
        action=on_conflict_cfg.action,
        update_columns=db_update_columns,
    )

    if isinstance(stmt, GenericUpsertPlan):
        pk_value = await self._execute_generic_upsert_plan(session, model, pk_config, stmt)
    else:
        await session.execute(stmt)
        pk_value = await self._lookup_pk_by_conflict_key(
            session, model, pk_config, db_conflict_key, row_data
        )

    # Decide action + field_changes based on whether existing_row was present
    if existing_row is None:
        action = "insert"
        field_changes = None
    elif on_conflict_cfg.action == "skip":
        action = "skip"
        field_changes = None
    else:
        # action == "update": compute diff
        field_changes = self._compute_upsert_changes(
            row_data, existing_row, db_conflict_key, db_update_columns, model
        )
        if not field_changes:
            # No real change — treat as skip for audit purposes
            action = "skip"
            field_changes = None
        else:
            action = "update"

    return pk_value, action, field_changes


async def _lookup_existing_row(
    self, session, model, db_conflict_key: list[str], row_data: dict
) -> dict | None:
    """Return a dict of {column: value} for the existing row, or None."""
    from sqlalchemy import select

    stmt = select(model)
    for col_name in db_conflict_key:
        stmt = stmt.where(getattr(model, col_name) == row_data[col_name])
    result = await session.execute(stmt)
    row = result.scalar_one_or_none()
    if row is None:
        return None
    return {col.name: getattr(row, col.name) for col in model.__table__.columns}


def _compute_upsert_changes(
    self,
    row_data: dict,
    existing_row: dict,
    key_columns: list[str],
    update_columns,
    model,
) -> dict[str, dict]:
    """Compute {column: {old, new}} for columns that differ in an upsert."""
    key_set = set(key_columns)

    if update_columns == "all":
        candidates = [
            col.name for col in model.__table__.columns
            if col.name not in key_set and col.name in row_data
        ]
    else:
        candidates = [c for c in update_columns if c in row_data]

    changes = {}
    for col_name in candidates:
        new_val = row_data.get(col_name)
        old_val = existing_row.get(col_name)
        if new_val != old_val:
            changes[col_name] = {"old": old_val, "new": new_val}
    return changes
```

(f) Update the call site in `_insert_one_record` to unpack the tuple and emit audit:

```python
            else:
                # Upsert path
                pk_value, action, field_changes = await self._execute_upsert(
                    session, model, table_name, row_data, pk_config, table_cfg.on_conflict
                )
                if audit_logger is not None:
                    audit_logger.record(AuditEntry(
                        target_table=table_name,
                        target_pk=str(pk_value),
                        action=action,
                        field_changes=field_changes,
                    ))
```

NOTE: This changes `_execute_upsert`'s return type from `pk_value` to `(pk_value, action, field_changes)`. There may be tests in `tests/db/test_inserter_upsert.py` that don't break because they only check final DB state, not the return value. But if any internal code paths read `_execute_upsert`'s return value as a scalar, they need updating. Search:

```bash
grep -n "_execute_upsert" siphon/db/inserter.py
```

There should only be the call site in `_insert_one_record`. If it appears elsewhere, update accordingly.

- [ ] **Step 4: Run new tests**

Run: `.venv/bin/pytest tests/db/test_inserter_audit.py -v`
Expected: 7 passed.

- [ ] **Step 5: Run all DB tests for regressions**

Run: `.venv/bin/pytest tests/db/ -q`
Expected: All db tests pass — no regressions in test_inserter.py, test_inserter_upsert.py, test_inserter_batched.py, test_run_tracker.py.

- [ ] **Step 6: Run full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add siphon/db/inserter.py tests/db/test_inserter_audit.py
git commit -m "feat: Inserter emits audit entries for insert/update/skip actions"
```

---

### Task 6: Pipeline — wire AuditLogger and `--user`

**Files:**
- Modify: `siphon/core/pipeline.py`
- Create: `tests/core/test_pipeline_audit.py`

The pipeline needs to:
1. Accept a `user: str | None` parameter on `run()`.
2. When `track_runs` AND `audit` are both true, create the `_siphon_audit` table after starting the run.
3. Pass an `AuditLogger(db_engine, run_id)` to the inserter.
4. After insertion completes, separately update each audit row's `reviewed_by` to the user value (if provided).

The cleanest approach: pass `reviewed_by` and `source_file` info into the `AuditLogger` itself so they're set on every entry it creates. Update `AuditLogger` to optionally hold these defaults.

- [ ] **Step 1: Add default fields to AuditLogger**

Modify `siphon/db/audit.py` to optionally accept defaults that are applied to every entry. Update `AuditLogger.__init__`:

```python
class AuditLogger:
    def __init__(
        self,
        db_engine: DatabaseEngine,
        run_id: int,
        *,
        source_file: str | None = None,
        reviewed_by: str | None = None,
    ) -> None:
        self._db = db_engine
        self._run_id = run_id
        self._default_source_file = source_file
        self._default_reviewed_by = reviewed_by
        self._buffer: list[AuditEntry] = []
```

In `record()`, fill in defaults if the entry doesn't have them:

```python
    def record(self, entry: AuditEntry) -> None:
        """Buffer an audit entry, applying defaults for missing optional fields."""
        if entry.source_file is None:
            entry.source_file = self._default_source_file
        if entry.reviewed_by is None:
            entry.reviewed_by = self._default_reviewed_by
        self._buffer.append(entry)
```

Add a quick test to `tests/db/test_audit.py`:

```python
class TestAuditLoggerDefaults:
    async def test_defaults_applied_to_entries(self, engine_setup):
        from sqlalchemy import select

        logger = AuditLogger(
            engine_setup, run_id=1,
            source_file="/tmp/data.csv",
            reviewed_by="alice",
        )
        await logger.create_audit_table()
        logger.record(AuditEntry(target_table="t", target_pk="1", action="insert"))
        await logger.flush()

        async with engine_setup.session() as session:
            row = (await session.execute(select(SiphonAudit))).scalar_one()
        assert row.source_file == "/tmp/data.csv"
        assert row.reviewed_by == "alice"

    async def test_explicit_entry_values_override_defaults(self, engine_setup):
        from sqlalchemy import select

        logger = AuditLogger(
            engine_setup, run_id=1,
            source_file="/tmp/default.csv",
            reviewed_by="default_user",
        )
        await logger.create_audit_table()
        logger.record(AuditEntry(
            target_table="t", target_pk="1", action="insert",
            source_file="/tmp/override.csv",
            reviewed_by="override_user",
        ))
        await logger.flush()

        async with engine_setup.session() as session:
            row = (await session.execute(select(SiphonAudit))).scalar_one()
        assert row.source_file == "/tmp/override.csv"
        assert row.reviewed_by == "override_user"
```

Run: `.venv/bin/pytest tests/db/test_audit.py -v`
Expected: 13 passed.

Commit:
```bash
git add siphon/db/audit.py tests/db/test_audit.py
git commit -m "feat: AuditLogger applies source_file and reviewed_by defaults"
```

- [ ] **Step 2: Write the failing pipeline tests**

Create `tests/core/test_pipeline_audit.py`:

```python
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
```

- [ ] **Step 3: Verify tests fail**

Run: `.venv/bin/pytest tests/core/test_pipeline_audit.py -v`
Expected: FAIL — `Pipeline.run()` doesn't accept `user`, and audit isn't being written.

- [ ] **Step 4: Modify the pipeline**

In `siphon/core/pipeline.py`:

(a) Add an import:
```python
from siphon.db.audit import AuditLogger
```

(b) Add `user: str | None = None` to `run()` signature:
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
    user: str | None = None,
) -> PipelineResult:
```

(c) In the run-tracking block (where `RunTracker` is set up), after `start_run` returns the `run_id`, set up the `AuditLogger`:

```python
            audit_logger = None
            if (
                self._config.pipeline.track_runs
                and self._config.pipeline.audit
                and run_id is not None
            ):
                audit_logger = AuditLogger(
                    db_engine,
                    run_id=run_id,
                    source_file=str(input_path),
                    reviewed_by=user,
                )
                await audit_logger.create_audit_table()
```

(d) Pass `audit_logger` to `inserter.insert(...)`:
```python
            try:
                result.total_inserted = await inserter.insert(
                    records_to_insert,
                    batch_size=self._config.pipeline.batch_size,
                    on_batch=_on_batch,
                    audit_logger=audit_logger,
                )
            except Exception as e:
                # ... existing error handling ...
```

The exact location depends on the current pipeline code from Phase 3. Read the current `siphon/core/pipeline.py` to confirm the structure, then place the audit-logger setup right after the run is started.

- [ ] **Step 5: Run new tests**

Run: `.venv/bin/pytest tests/core/test_pipeline_audit.py -v`
Expected: 5 passed.

- [ ] **Step 6: Run full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add siphon/core/pipeline.py tests/core/test_pipeline_audit.py
git commit -m "feat: pipeline writes audit entries with --user identification"
```

---

### Task 7: CLI — `--user` flag

**Files:**
- Modify: `siphon/cli.py`
- Modify: `tests/test_cli.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_cli.py`:

```python
class TestUserFlag:
    def test_user_flag_passed_to_pipeline(self, tmp_path):
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
                "--user", "alice",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs["user"] == "alice"

    def test_user_default_is_none(self, tmp_path):
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
        assert kwargs.get("user") is None
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/test_cli.py::TestUserFlag -v`
Expected: FAIL.

- [ ] **Step 3: Add the flag to `siphon/cli.py`**

In the `run` command, add this option alongside the others (e.g., next to `--resume`):

```python
    user: Optional[str] = typer.Option(None, "--user", help="Username for audit trail"),
```

In the `pipeline.run(...)` call, pass `user=user`:

```python
        result = asyncio.run(
            pipeline.run(
                input_path,
                dry_run=dry_run,
                no_review=no_review,
                create_tables=create_tables,
                sheet=sheet,
                resume=resume,
                user=user,
            )
        )
```

If a CLI test asserts the exact kwargs (e.g., `test_run_passes_pipeline_args`), update it to include `user=None`.

- [ ] **Step 4: Run new tests and full CLI suite**

Run: `.venv/bin/pytest tests/test_cli.py -v 2>&1 | tail -10`
Expected: All pass.

- [ ] **Step 5: Run full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/cli.py tests/test_cli.py
git commit -m "feat: --user flag for audit trail attribution"
```

---

### Task 8: End-to-end audit integration test

**Files:**
- Create: `tests/test_integration_audit.py`

- [ ] **Step 1: Write the test**

Create `tests/test_integration_audit.py`:

```python
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
        # - 1 update by bob (Acme: OLD→NEW) and 1 insert (Gamma) for run 2
        # - Beta's no-op update is NOT audited
        rows = await _query(
            db_path_url := f"sqlite+aiosqlite:///{db_path}",
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

        # The update should have field_changes with phone OLD→NEW
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
```

- [ ] **Step 2: Run the test**

Run: `.venv/bin/pytest tests/test_integration_audit.py -v`
Expected: 2 passed.

- [ ] **Step 3: Run full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration_audit.py
git commit -m "test: end-to-end audit trail integration test"
```

---

### Task 9: README + version bump + push

**Files:**
- Modify: `README.md`
- Modify: `siphon/__init__.py`

- [ ] **Step 1: Add an "Audit Trail" section to `README.md`**

Find the existing "## Resumable Runs" section. Insert the new section IMMEDIATELY AFTER it (before the next `##` heading). Use literal triple backticks in the actual file:

```markdown
## Audit Trail

Every record-level action (insert, update, skip) is logged in a `_siphon_audit` table. Use `--user NAME` to attribute imports to a person:

```bash
siphon run data.csv --user alice
```

Then anyone can answer "what got imported when, and by whom?" with a SQL query:

```sql
SELECT target_table, action, target_pk, reviewed_by, created_at
FROM _siphon_audit
WHERE created_at > '2026-04-01';
```

**What gets logged:**
- Every insert: target table, PK, source file, source row number
- Every update (from upserts): JSON of field-level changes (`{"col": {"old": ..., "new": ...}}`)
- Every skip (when `on_conflict.action: skip`): target table and PK
- The `--user NAME` value (or NULL if not provided)
- The `run_id` from `_siphon_runs` for cross-referencing

**No-op updates are not audited.** If an upsert results in zero field changes, no audit row is written — only meaningful changes are recorded.

**Configuration:**
```yaml
pipeline:
  audit: true       # default — disable with `audit: false`
```

The audit table is only written when both `track_runs` and `audit` are true (the default). Audit writes happen in a separate transaction from the data inserts, so audit failures won't break the import.
```

- [ ] **Step 2: Bump version**

In `siphon/__init__.py`, change:
```python
__version__ = "0.3.0a3"
```
to:
```python
__version__ = "0.3.0a4"
```

- [ ] **Step 3: Run the full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 4: Audit for proprietary references**

Run: `grep -ri "workshield\|work.shield" --include="*.py" --include="*.yaml" --include="*.md" --include="*.toml" .`
Expected: No matches in production code (matches in `docs/superpowers/plans/` are command quotations and can be ignored).

- [ ] **Step 5: Commit and push**

```bash
git add README.md siphon/__init__.py
git commit -m "chore: bump version to 0.3.0a4 (Phase 4 complete)"
git push -u origin v3-phase-4-audit-trail
```

---

## Verification

After all tasks complete:
1. All tests pass: `.venv/bin/pytest tests/ -q` shows zero failures.
2. Branch `v3-phase-4-audit-trail` is pushed to origin.
3. CLI smoke test: with audit enabled, run `siphon run data.csv --user alice --create-tables` and verify `_siphon_audit` contains insert rows attributed to alice.

## Out of Scope

- **Audit table cleanup**: rows accumulate indefinitely. Future phase or `--cleanup-audit` command can address this.
- **Audit for collection records**: only main-record actions are audited. Collection inserts (from nested XML/JSON) currently don't emit audit entries.
- **Audit for dedup-skips**: records skipped by in-batch dedup before reaching the inserter aren't audited (they never had a PK).
- **Schema versioning of audit format**: `field_changes` is JSON — if its shape changes in future versions, no migration tooling is provided.
- **Browsable UI**: this phase only writes audit rows. Phase 8 (Web UI) will surface them.

## Self-Review Notes

**Spec coverage:**
- ✅ `_siphon_audit` table with all 10 columns — Task 3
- ✅ Per-record action logging (insert/update/skip) — Task 5
- ✅ Field-level change tracking via JSON — Tasks 4, 5
- ✅ `run_id` foreign-key linking — Tasks 3, 6
- ✅ `source_file`, `source_row` fields — Task 6 (defaults via AuditLogger)
- ✅ `--user NAME` flag — Tasks 6, 7
- ✅ `audit: true` config (default true) — Task 2
- ✅ No-op updates not audited — Task 5

**Type consistency:**
- `AuditLogger(db_engine, run_id, *, source_file, reviewed_by)` — same in Tasks 3, 4, 6.
- `AuditEntry(target_table, target_pk, action, field_changes, source_file, source_row, reviewed_by)` — same across all tasks.
- `Inserter.insert(... audit_logger=None)` — same across Tasks 5, 6.
- `_execute_upsert(...) -> (pk_value, action, field_changes)` — return-type change documented in Task 5.

**Placeholder scan:** None.

**Known caveat:** The `_execute_upsert` return-type change in Task 5 is the riskiest piece. The implementer must search for callers and update them. The plan calls this out explicitly.
