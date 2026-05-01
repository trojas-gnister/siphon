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
