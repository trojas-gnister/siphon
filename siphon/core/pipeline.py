"""Pipeline orchestrator connecting all Siphon ETL components."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from siphon.config.schema import SiphonConfig
from siphon.core.mapper import Mapper
from siphon.core.review_cli import ReviewCLI
from siphon.core.reviewer import ReviewBatch, ReviewStatus
from siphon.core.validator import Validator
from siphon.db.differ import Differ
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator
from siphon.sources.spreadsheet import SpreadsheetLoader
from siphon.sources.xml import XMLLoader
from siphon.transforms.loader import load_custom_transforms
from siphon.utils.errors import ConfigError
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
    diff: dict | None = None


class Pipeline:
    """Orchestrates the full Siphon ETL pipeline.

    Stages: load -> map -> validate -> deduplicate -> (review) -> insert.
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
        sheet: str | int | None = None,
    ) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            input_path: Path to source file or directory of source files.
            dry_run: If True, load + map + validate only, no DB insertion.
            no_review: If True, skip HITL review.
            create_tables: If True, auto-create tables before insertion.
            sheet: Sheet name or 0-based index for multi-sheet Excel files.

        Returns:
            PipelineResult with counts and details.
        """
        # 1. Setup logging
        setup_logging(
            self._config.pipeline.log_level,
            self._config.pipeline.log_dir,
        )

        result = PipelineResult(dry_run=dry_run)

        # 2. Load custom transforms (if configured)
        custom_transforms: dict[str, callable] = {}
        if self._config.transforms and self._config.transforms.file:
            custom_transforms = load_custom_transforms(
                self._config.transforms.file
            )

        # 3. Load source data
        source_config = self._config.source
        if source_config.type == "spreadsheet":
            loader = SpreadsheetLoader()
            input_path = Path(input_path)

            if input_path.is_dir():
                files = self._scan_directory(input_path)
                if not files:
                    logger.warning(
                        "No supported files found in %s", input_path
                    )
                    return result

                source_records: list[dict] = []
                for f in files:
                    logger.info("Processing %s", f)
                    source_records.extend(loader.load(f, sheet=sheet))
            else:
                logger.info("Loading data from %s", input_path)
                source_records = loader.load(input_path, sheet=sheet)

        elif source_config.type == "xml":
            loader = XMLLoader(
                root=source_config.root,
                encoding=source_config.encoding,
                force_list=source_config.force_list,
            )
            logger.info("Loading data from %s", input_path)
            source_records = loader.load(input_path)

        else:
            raise ConfigError(
                f"Unsupported source type: {source_config.type}"
            )

        if not source_records:
            logger.warning("No records loaded — nothing to process")
            return result

        # 4. Map source records to target schema
        mapper = Mapper(self._config, custom_transforms)
        records = mapper.map_records(source_records)
        result.total_extracted = len(records)

        # 5. Map collections (if any)
        all_collection_records: dict[str, list[dict]] = defaultdict(list)
        if self._config.schema_.collections:
            for source_rec, mapped_rec in zip(source_records, records):
                collections = mapper.map_collections(source_rec, mapped_rec)
                for name, items in collections.items():
                    all_collection_records[name].extend(items)

        if not records:
            logger.warning("No records after mapping — nothing to process")
            return result

        # 6. Validate main records
        logger.info("Validating %d records", len(records))
        validator = Validator(self._config)
        valid_records, invalid_records = validator.validate_records(records)
        result.total_valid = len(valid_records)
        result.total_invalid = len(invalid_records)
        result.invalid_records = invalid_records

        if invalid_records:
            for inv in invalid_records:
                logger.warning(
                    "Validation failed for record: %s", inv["errors"]
                )

        if not valid_records:
            logger.warning("No valid records after validation")
            return result

        # 7. Deduplicate
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
                            case_insensitive = (
                                dedup_config.match == "case_insensitive"
                            )
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

        # 8. If dry_run, compute diff then return
        if dry_run:
            # Compute the diff against current DB state.
            # Note: this requires a DB connection and the model_gen, but
            # never writes anything.
            db_engine = DatabaseEngine(self._config.database)
            try:
                model_gen = ModelGenerator(self._config)
                model_gen.generate()
                differ = Differ(self._config, db_engine, model_gen)
                try:
                    result.diff = await differ.compute_diff(valid_records)
                except Exception as e:
                    # If the DB doesn't exist yet (e.g., create_tables=False
                    # and no DB), fall back to "everything is an insert".
                    logger.warning("Diff computation failed: %s", e)
                    result.diff = {
                        "insert": list(valid_records),
                        "update": [],
                        "skip": [],
                        "no_change": [],
                    }
            finally:
                await db_engine.dispose()
            logger.info("Dry run complete — no database operations performed")
            return result

        if not valid_records:
            logger.warning("No records to insert after deduplication")
            return result

        # 9. Prepare DB components (needed for both review and insertion)
        db_engine = DatabaseEngine(self._config.database)
        try:
            model_gen = ModelGenerator(self._config)
            model_gen.generate()

            if create_tables:
                await db_engine.create_tables(model_gen.base)
            else:
                table_names = list(self._config.schema_.tables.keys())
                for rel in self._config.relationships:
                    if hasattr(rel, "through"):
                        table_names.append(rel.through)
                await db_engine.verify_tables(table_names)

            inserter = Inserter(self._config, db_engine, model_gen)
            await inserter.load_existing_keys()

            # 10. Review (human-in-the-loop)
            if not no_review and self._config.pipeline.review:
                batch = ReviewBatch(
                    records=valid_records,
                    llm_client=None,
                    config=self._config,
                    inserter=inserter,
                )
                review_cli = ReviewCLI()
                batch = await review_cli.run_review(batch)

                if batch.status == ReviewStatus.REJECTED:
                    logger.info("Batch rejected — no records inserted")
                    return result

                # Use the (possibly revised) records from the approved batch
                valid_records = batch.records
                result.total_valid = len(valid_records)

            # 11. Insert main records (target only top-level field tables)
            main_tables = {f.db.table for f in self._config.schema_.fields}
            result.total_inserted = await inserter.insert(
                valid_records, target_tables=main_tables
            )

            # 12. Insert collection records (if any)
            if all_collection_records and self._config.schema_.collections:
                # Build a map of collection name -> set of target tables
                coll_tables: dict[str, set[str]] = {}
                for coll in self._config.schema_.collections:
                    tables = {f.db.table for f in coll.fields}
                    coll_tables[coll.name] = tables

                for coll_name, coll_records in all_collection_records.items():
                    if coll_records:
                        target = coll_tables.get(coll_name)
                        await inserter.insert(
                            coll_records, target_tables=target
                        )

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
