"""Tests for the dialect-aware upsert statement builder."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table


def _make_table():
    """Build a simple SQLAlchemy Table for testing."""
    md = MetaData()
    return Table(
        "companies",
        md,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), unique=True),
        Column("phone", String(20)),
        Column("website", String(500)),
    )


class TestDetectDialect:
    def test_sqlite_dialect(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("sqlite+aiosqlite:///x.db") == "sqlite"

    def test_postgres_dialect(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("postgresql+asyncpg://u:p@h/d") == "postgresql"

    def test_mysql_dialect(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("mysql+aiomysql://u:p@h/d") == "mysql"

    def test_unknown_dialect_returns_generic(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("oracle://u:p@h/d") == "generic"


class TestBuildUpsertSQLite:
    def test_action_update_all_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "INSERT INTO companies" in compiled
        assert "ON CONFLICT (name) DO UPDATE" in compiled
        assert "phone" in compiled
        assert "website" in compiled

    def test_action_update_specific_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns=["phone"],
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "DO UPDATE SET phone" in compiled
        assert "website" not in compiled.split("DO UPDATE")[1]

    def test_action_skip_uses_do_nothing(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT (name) DO NOTHING" in compiled

    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT" not in compiled

    def test_composite_conflict_key(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name", "phone"],
            action="update",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT (name, phone)" in compiled


class TestBuildUpsertPostgres:
    def test_action_update_all_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="postgresql",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        from sqlalchemy.dialects import postgresql
        compiled = str(stmt.compile(dialect=postgresql.dialect()))
        assert "INSERT INTO companies" in compiled
        assert "ON CONFLICT (name) DO UPDATE" in compiled

    def test_action_skip(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="postgresql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        from sqlalchemy.dialects import postgresql
        compiled = str(stmt.compile(dialect=postgresql.dialect()))
        assert "ON CONFLICT (name) DO NOTHING" in compiled

    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="postgresql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        from sqlalchemy.dialects import postgresql
        compiled = str(stmt.compile(dialect=postgresql.dialect()))
        assert "ON CONFLICT" not in compiled


class TestBuildUpsertMySQL:
    def test_action_update_all_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "INSERT INTO companies" in compiled
        assert "ON DUPLICATE KEY UPDATE" in compiled

    def test_action_update_specific_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns=["phone"],
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY UPDATE" in compiled
        update_part = compiled.split("ON DUPLICATE KEY UPDATE")[1]
        assert "phone" in update_part
        assert "website" not in update_part

    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY" not in compiled

    def test_action_skip_uses_no_op_update(self):
        """MySQL has no DO NOTHING; emulate by setting a column to itself."""
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY UPDATE" in compiled
        assert "name = name" in compiled or "name=name" in compiled


class TestBuildUpsertGeneric:
    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="generic",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "INSERT INTO companies" in compiled
        assert "ON CONFLICT" not in compiled
        assert "ON DUPLICATE KEY" not in compiled

    def test_action_update_returns_marker_for_caller(self):
        """Generic fallback returns a special object signalling 'use select-then-update'."""
        from siphon.db.upsert import build_upsert_statement, GenericUpsertPlan
        table = _make_table()
        plan = build_upsert_statement(
            dialect="generic",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        assert isinstance(plan, GenericUpsertPlan)
        assert plan.action == "update"
        assert plan.conflict_key == ["name"]
        assert plan.row == {"name": "Acme", "phone": "555"}
        assert plan.update_columns == "all"
        assert plan.table is table

    def test_action_skip_returns_marker(self):
        from siphon.db.upsert import build_upsert_statement, GenericUpsertPlan
        table = _make_table()
        plan = build_upsert_statement(
            dialect="generic",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        assert isinstance(plan, GenericUpsertPlan)
        assert plan.action == "skip"
