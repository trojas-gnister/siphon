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
