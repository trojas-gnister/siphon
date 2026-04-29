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
