"""Human-in-the-loop review batch for the Siphon ETL pipeline."""

from __future__ import annotations

import enum
import logging

from siphon.config.schema import SiphonConfig
from siphon.db.inserter import Inserter
from siphon.utils.errors import ReviewError

logger = logging.getLogger("siphon")


class ReviewStatus(enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ReviewBatch:
    """Presentation-agnostic API for human-in-the-loop review of extracted records.

    Provides approve/reject actions and summary/preview helpers.
    The Rich CLI renderer consumes this API.
    """

    def __init__(
        self,
        records: list[dict],
        config: SiphonConfig,
        inserter: Inserter | None = None,
    ) -> None:
        self._records = records
        self._config = config
        self._inserter = inserter
        self._status = ReviewStatus.PENDING

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def records(self) -> list[dict]:
        return self._records

    @property
    def status(self) -> ReviewStatus:
        return self._status

    # ------------------------------------------------------------------
    # Review actions
    # ------------------------------------------------------------------

    def approve(self) -> None:
        """Approve the batch for insertion."""
        self._status = ReviewStatus.APPROVED
        logger.info("Batch approved for insertion")

    def reject(self) -> None:
        """Reject the batch — no insertion will occur."""
        self._status = ReviewStatus.REJECTED
        logger.info("Batch rejected")

    # ------------------------------------------------------------------
    # Summary / preview helpers
    # ------------------------------------------------------------------

    def get_summary(self) -> dict:
        """Return a summary dict of the batch."""
        tables = set()
        for field in self._config.schema_.fields:
            tables.add(field.db.table)

        return {
            "record_count": len(self._records),
            "tables_affected": sorted(tables),
            "status": self._status.value,
        }

    def get_sql_preview(self) -> list[str]:
        """Generate SQL INSERT preview statements (at most 5 records)."""
        if self._inserter:
            return self._inserter.generate_sql_preview(self._records)

        # Fallback: generate simple INSERT statements without an Inserter
        statements = []
        table_fields: dict[str, list] = {}
        for field in self._config.schema_.fields:
            table_fields.setdefault(field.db.table, []).append(field)

        for record in self._records[:5]:
            for table_name, fields in table_fields.items():
                cols = []
                vals = []
                for field in fields:
                    value = record.get(field.name)
                    if value is not None:
                        cols.append(field.db.column)
                        vals.append(repr(value))
                if cols:
                    stmt = (
                        f"INSERT INTO {table_name} "
                        f"({', '.join(cols)}) "
                        f"VALUES ({', '.join(vals)})"
                    )
                    statements.append(stmt)

        return statements
