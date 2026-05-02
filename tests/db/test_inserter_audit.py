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
    # ON CONFLICT requires a unique constraint to fire
    table = model_gen.models["items"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_items_name"))
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
        # Add a unique index to force failure (constraint via raw SQL since
        # the table already exists)
        from sqlalchemy import text
        async with engine.engine.begin() as conn:
            await conn.execute(text("CREATE UNIQUE INDEX uq_items_name ON items(name)"))

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
