"""Pipeline orchestrator connecting all Siphon ETL components."""

from __future__ import annotations

import hashlib
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from siphon.config.schema import SiphonConfig
from siphon.core.mapper import Mapper
from siphon.core.review_cli import ReviewCLI
from siphon.core.reviewer import ReviewBatch, ReviewStatus
from siphon.core.validator import Validator
from siphon.db.audit import AuditLogger
from siphon.db.differ import Differ
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator
from siphon.db.run_tracker import RunTracker
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

    def _effective_fields(self):
        """Return the flat list of FieldConfig used by validate/dedup/insert.

        - Single-source: config.schema_.fields
        - Multi-source: concatenation of each source's fields (in source order),
          filtered to only those with a `db` mapping. Fields without `db` are
          treated as join-key intermediates: they exist so the per-source
          mapper produces the join key in the mapped record, but they are not
          themselves persisted.
        """
        if self._config.sources:
            flat = []
            for src in self._config.sources:
                if src.fields:
                    for f in src.fields:
                        if f.db is not None:
                            flat.append(f)
            return flat
        return self._config.schema_.fields or []

    async def _load_source(self, src):
        """Load raw records from a single SourceConfig (with its own path).

        Used for multi-source mode where each source has a path declared
        in the YAML.
        """
        if not src.path:
            raise ConfigError(
                f"Source '{src.name}' has no path declared"
            )

        if src.type == "spreadsheet":
            loader = SpreadsheetLoader()
            return loader.load(src.path)
        elif src.type == "xml":
            loader = XMLLoader(
                root=src.root,
                encoding=src.encoding,
                force_list=src.force_list,
            )
            return loader.load(src.path)
        else:
            raise ConfigError(f"Unsupported source type: {src.type}")

    def _build_source_mapper(self, src, custom_transforms: dict):
        """Build a Mapper that operates only on this source's fields.

        Creates a transient SiphonConfig view with `schema_.fields = src.fields`
        so the existing Mapper class works unchanged. Collections are dropped
        in this view — collections are only supported on single-source XML/JSON.
        """
        cfg_view = self._config.model_copy(deep=True)
        cfg_view.schema_.fields = list(src.fields or [])
        cfg_view.schema_.collections = None
        return Mapper(cfg_view, custom_transforms)

    def _config_hash(self) -> str:
        """Stable SHA-256 hash of the config (excluding fields that don't affect data)."""
        payload = self._config.model_dump(by_alias=True, mode="json")
        # Remove fields that don't affect data — log_level, log_dir, etc.
        payload.get("pipeline", {}).pop("log_level", None)
        payload.get("pipeline", {}).pop("log_dir", None)
        serialized = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    @staticmethod
    def _scan_directory(directory: Path) -> list[Path]:
        """Find all supported spreadsheet files in a directory."""
        supported_extensions = {".csv", ".xlsx", ".xls", ".ods"}
        files = []
        for f in sorted(directory.iterdir()):
            if f.is_file() and f.suffix.lower() in supported_extensions:
                files.append(f)
        return files

    async def _load_and_map_records(
        self,
        input_path: str | Path,
        sheet: str | int | None,
    ) -> tuple[list[dict], dict[str, list[dict]], str]:
        """Load source data, map to target schema, apply joins.

        Handles both single-source and multi-source modes. For multi-source,
        mutates self._config.schema_.fields to the flat union view so downstream
        components work unchanged.

        Logs the same "no records" warnings as the original inline block when
        loading or mapping yields zero records; the caller is expected to
        short-circuit on an empty records list.

        Returns (records, all_collection_records, effective_input_path).
        """
        # Load custom transforms (if configured)
        custom_transforms: dict[str, callable] = {}
        if self._config.transforms and self._config.transforms.file:
            custom_transforms = load_custom_transforms(
                self._config.transforms.file
            )

        # Load source data — dispatch on single-source vs multi-source
        if self._config.sources:
            # ===== MULTI-SOURCE MODE =====
            from siphon.core.joiner import join_records

            # Normalize: collapse all sources' fields into schema_.fields so
            # downstream validator/model_gen/inserter work unchanged.
            self._config.schema_.fields = self._effective_fields()

            # Load + map each source independently
            mapped_per_source: dict[str, list[dict]] = {}
            for src in self._config.sources:
                logger.info("Loading source '%s' from %s", src.name, src.path)
                raw = await self._load_source(src)
                src_mapper = self._build_source_mapper(src, custom_transforms)
                mapped_per_source[src.name] = src_mapper.map_records(raw)
                logger.info(
                    "Source '%s': loaded %d records, mapped to %d",
                    src.name, len(raw), len(mapped_per_source[src.name]),
                )

            # Apply joins in order
            if self._config.joins:
                first_left = self._config.joins[0].left
                merged = list(mapped_per_source[first_left])
                for j in self._config.joins:
                    if j.left == first_left:
                        # Extending the running merged set
                        right_records = mapped_per_source[j.right]
                        merged = join_records(
                            merged, right_records,
                            on=j.on, join_type=j.type,
                        )
                    else:
                        # Independent side join: union with the running set
                        side = join_records(
                            mapped_per_source[j.left],
                            mapped_per_source[j.right],
                            on=j.on, join_type=j.type,
                        )
                        merged.extend(side)
                records = merged
            else:
                # No joins declared: just union all sources' records
                records = []
                for src_records in mapped_per_source.values():
                    records.extend(src_records)

            # Multi-source mode does not support collections (collections are
            # for nested XML/JSON within a single source).
            all_collection_records: dict[str, list[dict]] = defaultdict(list)

            # input_path no longer determines source paths — but pipeline result
            # tracking still needs a value. Use a synthetic name describing the
            # multi-source setup.
            effective_input_path = (
                f"<multi-source: {', '.join(s.name for s in self._config.sources)}>"
            )

            if not records:
                logger.warning("No records after mapping — nothing to process")
                return records, all_collection_records, effective_input_path

        else:
            # ===== SINGLE-SOURCE MODE — existing behaviour =====
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
                        return [], defaultdict(list), str(input_path)

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
                return [], defaultdict(list), str(input_path)

            # Map source records to target schema
            mapper = Mapper(self._config, custom_transforms)
            records = mapper.map_records(source_records)

            # Map collections (if any)
            all_collection_records = defaultdict(list)
            if self._config.schema_.collections:
                for source_rec, mapped_rec in zip(source_records, records):
                    collections = mapper.map_collections(source_rec, mapped_rec)
                    for name, items in collections.items():
                        all_collection_records[name].extend(items)

            effective_input_path = str(input_path)

            if not records:
                logger.warning("No records after mapping — nothing to process")
                return records, all_collection_records, effective_input_path

        return records, all_collection_records, effective_input_path

    async def _handle_dry_run(
        self,
        result: PipelineResult,
        valid_records: list[dict],
    ) -> None:
        """Compute the dry-run diff and store it on result.

        Does not perform any inserts. Mutates result.diff. Falls back to
        "everything is an insert" if the diff computation raises (e.g., the
        target DB does not exist yet).
        """
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

    async def run(
        self,
        input_path: str | Path,
        *,
        dry_run: bool = False,
        no_review: bool = False,
        create_tables: bool = False,
        sheet: str | int | None = None,
        resume: bool = False,
        user: str | None = None,
    ) -> PipelineResult:
        """Execute the full pipeline.

        Args:
            input_path: Path to source file or directory of source files.
            dry_run: If True, load + map + validate only, no DB insertion.
            no_review: If True, skip HITL review.
            create_tables: If True, auto-create tables before insertion.
            sheet: Sheet name or 0-based index for multi-sheet Excel files.
            resume: If True, look up the most recent failed run for this
                pipeline+source+config and skip already-processed records.

        Returns:
            PipelineResult with counts and details.
        """
        # 1. Setup logging
        setup_logging(
            self._config.pipeline.log_level,
            self._config.pipeline.log_dir,
        )

        result = PipelineResult(dry_run=dry_run)

        # 2-3. Load source data + map records (single-source or multi-source).
        records, all_collection_records, effective_input_path = (
            await self._load_and_map_records(input_path, sheet)
        )
        result.total_extracted = len(records)
        if not records:
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
            await self._handle_dry_run(result, valid_records)
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

            run_tracker = None
            run_id = None
            records_to_insert = valid_records

            if self._config.pipeline.track_runs:
                run_tracker = RunTracker(db_engine)
                await run_tracker.create_runs_table()

                if resume:
                    resumable = await run_tracker.find_resumable_run(
                        pipeline_name=self._config.name,
                        source_file=effective_input_path,
                        config_hash=self._config_hash(),
                    )
                    if resumable is not None:
                        skip_n = resumable.processed_count
                        logger.info(
                            "Resuming from run id=%s — skipping first %d records",
                            resumable.id, skip_n,
                        )
                        records_to_insert = valid_records[skip_n:]
                    else:
                        logger.info("No resumable run found — running from start")

                run_id = await run_tracker.start_run(
                    pipeline_name=self._config.name,
                    source_file=effective_input_path,
                    total_records=len(valid_records),
                    config_hash=self._config_hash(),
                )

            audit_logger = None
            if (
                self._config.pipeline.track_runs
                and self._config.pipeline.audit
                and run_id is not None
            ):
                audit_logger = AuditLogger(
                    db_engine,
                    run_id=run_id,
                    source_file=effective_input_path,
                    reviewed_by=user,
                )
                await audit_logger.create_audit_table()

            async def _on_batch(committed: int) -> None:
                if run_tracker is not None and run_id is not None:
                    # processed_count is relative to the original valid_records,
                    # so add the number of skipped records.
                    skipped = len(valid_records) - len(records_to_insert)
                    await run_tracker.update_progress(run_id, skipped + committed)

            try:
                result.total_inserted = await inserter.insert(
                    records_to_insert,
                    target_tables=main_tables,
                    batch_size=self._config.pipeline.batch_size,
                    on_batch=_on_batch,
                    audit_logger=audit_logger,
                )
            except Exception as e:
                if run_tracker is not None and run_id is not None:
                    await run_tracker.fail_run(run_id, str(e))
                raise

            if run_tracker is not None and run_id is not None:
                await run_tracker.complete_run(run_id)

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
