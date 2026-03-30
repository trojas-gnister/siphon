"""Async SQLAlchemy database engine for the Siphon ETL pipeline."""

import logging

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from siphon.config.schema import DatabaseConfig
from siphon.utils.errors import DatabaseError

logger = logging.getLogger("siphon")


class Base(DeclarativeBase):
    """Base class for all dynamic ORM models."""

    pass


class DatabaseEngine:
    def __init__(self, config: DatabaseConfig):
        self._config = config
        try:
            self._engine = create_async_engine(
                config.url,
                echo=False,  # SQL logging handled separately
            )
        except Exception as e:
            raise DatabaseError(f"Failed to create engine: {e}") from e

        self._session_factory = async_sessionmaker(
            self._engine, class_=AsyncSession, expire_on_commit=False
        )

    @property
    def engine(self):
        return self._engine

    def session(self) -> AsyncSession:
        """Create a new async session."""
        return self._session_factory()

    async def create_tables(self, base: type[DeclarativeBase] = Base):
        """Create all tables defined in the metadata.

        Uses base.metadata.create_all() with the async engine.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(base.metadata.create_all)
        logger.info("Database tables created")

    async def verify_tables(self, table_names: list[str]):
        """Verify that all expected tables exist in the database.

        Raises DatabaseError if any tables are missing.
        """
        async with self._engine.connect() as conn:

            def _get_tables(sync_conn):
                inspector = inspect(sync_conn)
                return inspector.get_table_names()

            existing = await conn.run_sync(_get_tables)

        missing = set(table_names) - set(existing)
        if missing:
            raise DatabaseError(
                f"Missing tables: {sorted(missing)}. "
                f"Use --create-tables to auto-create them."
            )

    async def dispose(self):
        """Dispose of the engine and close all connections."""
        await self._engine.dispose()
        logger.info("Database engine disposed")
