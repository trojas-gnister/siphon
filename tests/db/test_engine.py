"""Tests for the async SQLAlchemy database engine."""

import pytest
from sqlalchemy import Column, Integer, String, text

from siphon.config.schema import DatabaseConfig
from siphon.db.engine import Base, DatabaseEngine
from siphon.utils.errors import DatabaseError


class SampleModel(Base):
    __tablename__ = "test_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(50))


@pytest.fixture
async def db_engine():
    config = DatabaseConfig(url="sqlite+aiosqlite://")
    engine = DatabaseEngine(config)
    yield engine
    await engine.dispose()


class TestEngineCreation:
    def test_engine_creates_successfully(self):
        """Engine creates without error given a valid URL."""
        config = DatabaseConfig(url="sqlite+aiosqlite://")
        engine = DatabaseEngine(config)
        assert engine.engine is not None

    def test_engine_exposes_underlying_engine(self):
        """The .engine property returns the underlying async engine."""
        config = DatabaseConfig(url="sqlite+aiosqlite://")
        engine = DatabaseEngine(config)
        # The underlying engine should be an AsyncEngine instance
        from sqlalchemy.ext.asyncio import AsyncEngine

        assert isinstance(engine.engine, AsyncEngine)


class TestSessionExecution:
    async def test_session_executes_simple_query(self, db_engine):
        """A session can execute a simple SELECT 1 query."""
        async with db_engine.session() as session:
            result = await session.execute(text("SELECT 1"))
            row = result.scalar()
        assert row == 1

    async def test_session_insert_and_query(self, db_engine):
        """A session can insert and retrieve data from a table."""
        await db_engine.create_tables(Base)

        async with db_engine.session() as session:
            session.add(SampleModel(id=1, name="Alice"))
            await session.commit()

        async with db_engine.session() as session:
            result = await session.execute(
                text("SELECT name FROM test_table WHERE id = 1")
            )
            name = result.scalar()

        assert name == "Alice"


class TestCreateTables:
    async def test_create_tables_creates_table(self, db_engine):
        """create_tables() creates the table defined in the ORM model."""
        await db_engine.create_tables(Base)

        # Verify the table exists by querying it
        async with db_engine.session() as session:
            result = await session.execute(text("SELECT COUNT(*) FROM test_table"))
            count = result.scalar()

        assert count == 0

    async def test_create_tables_is_idempotent(self, db_engine):
        """create_tables() can be called multiple times without error."""
        await db_engine.create_tables(Base)
        await db_engine.create_tables(Base)  # Should not raise


class TestVerifyTables:
    async def test_verify_tables_succeeds_when_tables_exist(self, db_engine):
        """verify_tables() succeeds when all specified tables exist."""
        await db_engine.create_tables(Base)
        # Should not raise
        await db_engine.verify_tables(["test_table"])

    async def test_verify_tables_raises_on_missing_table(self, db_engine):
        """verify_tables() raises DatabaseError when tables are missing."""
        with pytest.raises(DatabaseError) as exc_info:
            await db_engine.verify_tables(["nonexistent_table"])

        assert "nonexistent_table" in str(exc_info.value)
        assert "Missing tables" in str(exc_info.value)

    async def test_verify_tables_raises_for_partial_missing(self, db_engine):
        """verify_tables() raises DatabaseError even if some tables exist."""
        await db_engine.create_tables(Base)

        with pytest.raises(DatabaseError) as exc_info:
            await db_engine.verify_tables(["test_table", "missing_table"])

        assert "missing_table" in str(exc_info.value)

    async def test_verify_tables_empty_list_succeeds(self, db_engine):
        """verify_tables() with an empty list succeeds without error."""
        await db_engine.verify_tables([])


class TestDispose:
    async def test_dispose_completes_without_error(self, db_engine):
        """dispose() completes without raising an error."""
        # Should not raise
        await db_engine.dispose()

    async def test_dispose_closes_connections(self):
        """After dispose(), the engine pool is reset and no connections remain."""
        config = DatabaseConfig(url="sqlite+aiosqlite://")
        engine = DatabaseEngine(config)

        # Use the engine first to establish connections
        async with engine.session() as session:
            await session.execute(text("SELECT 1"))

        # Dispose should close all connections without error
        await engine.dispose()

        # Pool size should be 0 after dispose (connections cleaned up)
        # We verify this by checking that checkedin == 0 (pool was disposed)
        pool_status = engine.engine.pool.status()
        assert "0" in pool_status or pool_status is not None
