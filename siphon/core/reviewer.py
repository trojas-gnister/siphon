"""Human-in-the-loop review batch for the Siphon ETL pipeline."""

from __future__ import annotations

import enum
import json
import logging

from siphon.config.schema import SiphonConfig
from siphon.core.validator import Validator
from siphon.db.inserter import Inserter
from siphon.llm.client import LLMClient
from siphon.llm.prompts import build_revision_prompt
from siphon.utils.errors import ReviewError

logger = logging.getLogger("siphon")


class ReviewStatus(enum.Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class ReviewBatch:
    """Presentation-agnostic API for human-in-the-loop review of extracted records.

    Provides approve/reject/revise actions and summary/preview helpers.
    The Rich CLI renderer (Task 19) consumes this API.
    """

    def __init__(
        self,
        records: list[dict],
        llm_client: LLMClient,
        config: SiphonConfig,
        inserter: Inserter | None = None,
    ) -> None:
        self._records = records
        self._llm = llm_client
        self._config = config
        self._inserter = inserter
        self._status = ReviewStatus.PENDING
        self._revision_count = 0

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def records(self) -> list[dict]:
        return self._records

    @property
    def status(self) -> ReviewStatus:
        return self._status

    @property
    def revision_count(self) -> int:
        return self._revision_count

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

    async def revise(self, command: str) -> "ReviewBatch":
        """Revise the batch via LLM with a natural language command.

        Sends current records + user command to LLM, re-validates
        the response, and returns a new ReviewBatch with updated records.

        Raises ReviewError if revision fails.
        """
        try:
            batch_json = json.dumps(self._records, indent=2, default=str)
            prompt = build_revision_prompt(batch_json, command)
            revised_records = await self._llm.extract_json(prompt)
        except Exception as e:
            raise ReviewError(f"Revision failed: {e}") from e

        # Re-validate revised records
        validator = Validator(self._config)
        valid, invalid = validator.validate_records(revised_records)

        if invalid:
            logger.warning(f"Revision produced {len(invalid)} invalid records")

        # Create new ReviewBatch with the valid revised records
        new_batch = ReviewBatch(
            records=valid,
            llm_client=self._llm,
            config=self._config,
            inserter=self._inserter,
        )
        new_batch._revision_count = self._revision_count + 1
        return new_batch

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
            "revision_count": self._revision_count,
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
