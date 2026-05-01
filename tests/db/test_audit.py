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
