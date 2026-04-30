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
