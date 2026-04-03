"""Pipeline orchestrator connecting all Siphon ETL components."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from siphon.config.schema import SiphonConfig
from siphon.core.extractor import Extractor
from siphon.core.validator import Validator
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator
from siphon.llm.client import LLMClient
from siphon.utils.logger import setup_logging

logger = logging.getLogger("siphon")


@dataclass
class PipelineResult:
    """Result summary from a pipeline run."""

    total_extracted: int = 0
    total_valid: int = 0
    total_invalid: int = 0
    total_duplicates: int = 0
    total_inserted: int = 0
    skipped_chunks: list[dict] = field(default_factory=list)
    invalid_records: list[dict] = field(default_factory=list)
    duplicate_records: list[dict] = field(default_factory=list)
    dry_run: bool = False


class Pipeline:
    """Orchestrates the full Siphon ETL pipeline.

    Stages: extract -> validate -> deduplicate -> (review) -> insert.
    """

    def __init__(self, config: SiphonConfig) -> None:
        self._config = config

    @staticmethod
    def _scan_directory(directory: Path) -> list[Path]:
        """Find all supported spreadsheet files in a directory."""
        supported_extensions = {".csv", ".xlsx", ".xls", ".ods"}
        files = []
        for f in sorted(directory.iterdir()):
            if f.is_file() and f.suffix.lower() in supported_extensions:
                files.append(f)
        return files

    async def run(
        self,
        input_path: str | Path,
        *,
        dry_run: bool = False,
        no_review: bool = False,
        create_tables: bool = False,
        chunk_size: int | None = None,
    ) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            input_path: Path to spreadsheet file or directory of spreadsheet files.
            dry_run: If True, extract + validate only, no DB insertion.
            no_review: If True, skip HITL review.
            create_tables: If True, auto-create tables before insertion.
            chunk_size: Override config chunk_size.

        Returns:
            PipelineResult with counts and details.
        """
        # Setup logging
        setup_logging(
            self._config.pipeline.log_level,
            self._config.pipeline.log_dir,
        )

        result = PipelineResult(dry_run=dry_run)

        # Override chunk size if provided
        if chunk_size is not None:
            self._config.pipeline.chunk_size = chunk_size

        # Initialize components
        llm_client = LLMClient(self._config.llm)
        extractor = Extractor(self._config, llm_client)
        validator = Validator(self._config)

        # 1. Extract
        input_path = Path(input_path)

        if input_path.is_dir():
            files = self._scan_directory(input_path)
            if not files:
                logger.warning("No supported files found in %s", input_path)
                return result

            all_records: list[dict] = []
            all_skipped: list[dict] = []
            for file_path in files:
                logger.info("Processing %s", file_path)
                file_records, file_skipped = await extractor.extract(file_path)
                all_records.extend(file_records)
                all_skipped.extend(file_skipped)

            records = all_records
            skipped_chunks = all_skipped
        else:
            logger.info("Extracting data from %s", input_path)
            records, skipped_chunks = await extractor.extract(input_path)

        result.total_extracted = len(records)
        result.skipped_chunks = skipped_chunks

        if not records:
            logger.warning("No records extracted — nothing to process")
            return result

        # 2. Validate
        logger.info("Validating %d records", len(records))
        valid_records, invalid_records = validator.validate_records(records)
        result.total_valid = len(valid_records)
        result.total_invalid = len(invalid_records)
        result.invalid_records = invalid_records

        if not valid_records:
            logger.warning("No valid records after validation")
            return result

        # 3. Deduplicate
        if self._config.schema_.deduplication:
            dedup_config = self._config.schema_.deduplication
            existing_keys: set[tuple] | None = None

            if dedup_config.check_db and not dry_run:
                # Load existing keys from DB for dedup comparison
                db_engine = DatabaseEngine(self._config.database)
                try:
                    model_gen = ModelGenerator(self._config)
                    model_gen.generate()
                    inserter = Inserter(self._config, db_engine, model_gen)

                    # Query existing rows for the dedup key fields
                    async with db_engine.session() as session:
                        from sqlalchemy import select as sa_select

                        # Get the first table that contains dedup key fields
                        key_fields = dedup_config.key
                        # Find columns for each key field
                        table_name = self._config.schema_.fields[0].db.table
                        model = model_gen.models[table_name]

                        cols = []
                        for kf in key_fields:
                            for fc in self._config.schema_.fields:
                                if fc.name == kf:
                                    cols.append(getattr(model, fc.db.column))
                                    break

                        if cols:
                            stmt = sa_select(*cols)
                            rows_result = await session.execute(stmt)
                            rows = [
                                dict(zip(key_fields, row))
                                for row in rows_result.fetchall()
                            ]
                            case_insensitive = dedup_config.match == "case_insensitive"
                            existing_keys = Validator.build_existing_keys(
                                rows, key_fields, case_insensitive
                            )
                finally:
                    await db_engine.dispose()

            unique_records, duplicate_records = validator.deduplicate(
                valid_records, existing_keys
            )
            result.total_duplicates = len(duplicate_records)
            result.duplicate_records = duplicate_records
            valid_records = unique_records

        if dry_run:
            logger.info("Dry run complete — no database operations performed")
            return result

        if not valid_records:
            logger.warning("No records to insert after deduplication")
            return result

        # 4. Review (placeholder — Task 18-19 will implement ReviewBatch)
        # For now, if no_review is False and config.pipeline.review is True,
        # we would run the review. Since ReviewBatch doesn't exist yet,
        # just proceed to insertion.
        if not no_review and self._config.pipeline.review:
            logger.info("Review step requested but not yet implemented — proceeding")

        # 5. Insert
        db_engine = DatabaseEngine(self._config.database)
        try:
            model_gen = ModelGenerator(self._config)
            model_gen.generate()

            if create_tables:
                await db_engine.create_tables(model_gen.base)
            else:
                table_names = list(self._config.schema_.tables.keys())
                # Also include junction table names
                for rel in self._config.relationships:
                    if hasattr(rel, "through"):
                        table_names.append(rel.through)
                await db_engine.verify_tables(table_names)

            inserter = Inserter(self._config, db_engine, model_gen)
            await inserter.load_existing_keys()

            result.total_inserted = await inserter.insert(valid_records)
        finally:
            await db_engine.dispose()

        logger.info(
            "Pipeline complete: %d extracted, %d valid, %d invalid, "
            "%d duplicates, %d inserted",
            result.total_extracted,
            result.total_valid,
            result.total_invalid,
            result.total_duplicates,
            result.total_inserted,
        )

        return result
