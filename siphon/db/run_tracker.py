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
