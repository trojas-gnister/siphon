"""Relationship-aware record inserter with topological sort for the Siphon ETL pipeline."""

import asyncio
import logging
import uuid
from collections import defaultdict

from sqlalchemy import select

from siphon.config.schema import BelongsToRelationship, JunctionRelationship, SiphonConfig
from siphon.db.audit import AuditEntry, AuditLogger
from siphon.db.engine import DatabaseEngine
from siphon.db.models import ModelGenerator
from siphon.db.upsert import detect_dialect
from siphon.utils.errors import DatabaseError

logger = logging.getLogger("siphon")


class Inserter:
    """Insert validated records into the database respecting relationships.

    Handles:
    - Topological sort of tables (parents before children)
    - Self-referential belongs_to (record-level ordering)
    - FK resolution via a lookup cache
    - Junction row insertion
    - UUID PK generation
    - One transaction per batch (configurable via ``batch_size``); failing
      batch rolls back, previously committed batches persist.
    """

    def __init__(
        self,
        config: SiphonConfig,
        db_engine: DatabaseEngine,
        model_generator: ModelGenerator,
    ):
        self._config = config
        self._db = db_engine
        self._models = model_generator.models
        self._generator = model_generator
        self._dialect = detect_dialect(config.database.url)

        # Lookup cache: {table_name: {resolve_by_value: pk_value}}
        self._lookup_cache: dict[str, dict[str, any]] = defaultdict(dict)

        # Build the field-name → DB column-name map (used by upsert executor)
        name_to_column: dict[str, str] = {}
        for f in self._config.schema_.fields or []:
            if f.db:
                name_to_column[f.name] = f.db.column
        if self._config.schema_.collections:
            for coll in self._config.schema_.collections:
                for f in coll.fields:
                    if f.db:
                        name_to_column[f.name] = f.db.column

        from siphon.db.upsert_executor import UpsertExecutor
        self._upsert_executor = UpsertExecutor(
            dialect=self._dialect,
            field_name_to_column=name_to_column,
        )

    def topological_sort(self) -> list[str]:
        """Sort table names so parents come before children.

        Only considers data tables (not junction tables). Self-referential
        relationships are excluded from the graph (handled separately by
        record-level sorting).

        Raises:
            DatabaseError: If a circular dependency is detected.
        """
        from siphon.utils.graph import topological_sort as _topo_sort

        data_tables = list(self._config.schema_.tables.keys())
        edges: list[tuple[str, str]] = []

        for rel in self._config.relationships:
            if isinstance(rel, BelongsToRelationship):
                # Skip self-referential — handled at record level
                if rel.table != rel.references:
                    edges.append((rel.references, rel.table))

        try:
            return _topo_sort(data_tables, edges)
        except ValueError as e:
            raise DatabaseError(f"Circular dependency detected in table relationships: {e}") from e

    async def load_existing_keys(self):
        """Pre-populate lookup cache from existing DB rows for FK resolution."""
        for rel in self._config.relationships:
            if isinstance(rel, BelongsToRelationship):
                model = self._models[rel.references]
                pk_col = self._config.schema_.tables[rel.references].primary_key.column

                async with self._db.session() as session:
                    result = await session.execute(
                        select(
                            getattr(model, pk_col),
                            getattr(model, rel.resolve_by),
                        )
                    )
                    for pk_val, resolve_val in result:
                        self._lookup_cache[rel.references][resolve_val] = pk_val

    def _sort_records_for_self_ref(
        self, records: list[dict], rel: BelongsToRelationship
    ) -> list[dict]:
        """Sort records so parents come before children for self-referential relationships."""
        if rel.table != rel.references:
            return records

        # Find the config field name that maps to the resolve_by column
        resolve_field = None
        ref_field = rel.field
        for f in self._config.schema_.fields:
            if f.db.table == rel.references and f.db.column == rel.resolve_by:
                resolve_field = f.name
                break

        if resolve_field is None:
            return records

        # Topological sort: records with no parent (or parent already in cache) first
        sorted_records = []
        remaining = list(records)
        seen_values = set(self._lookup_cache.get(rel.references, {}).keys())

        max_iterations = len(remaining) + 1
        iteration = 0
        while remaining and iteration < max_iterations:
            iteration += 1
            next_remaining = []
            for record in remaining:
                parent_value = record.get(ref_field)
                if not parent_value or parent_value in seen_values:
                    sorted_records.append(record)
                    resolve_value = record.get(resolve_field)
                    if resolve_value:
                        seen_values.add(resolve_value)
                else:
                    next_remaining.append(record)
            if len(next_remaining) == len(remaining):
                # No progress -- add remaining (parent may not exist)
                sorted_records.extend(next_remaining)
                break
            remaining = next_remaining

        return sorted_records

    async def insert(
        self,
        records: list[dict],
        *,
        target_tables: set[str] | None = None,
        batch_size: int | None = None,
        on_batch=None,
        audit_logger: "AuditLogger | None" = None,
    ) -> int:
        """Insert records, optionally in batches with a per-batch progress callback.

        Args:
            records: List of mapped record dicts.
            target_tables: If provided, only insert into these tables.
                          Used for collection records that should only go
                          into their specific child table(s).
            batch_size: If set, commit every N records in a separate transaction.
                        If None, all records committed in one transaction (v2 behavior).
            on_batch: Optional callable invoked after each successful batch commit
                      with the cumulative committed count. Sync or async — both supported.

        Returns total number of records inserted across all batches.
        Raises DatabaseError on failure (the failing batch is rolled back; previously
        committed batches are NOT rolled back).
        """
        table_order = self.topological_sort()
        if target_tables is not None:
            table_order = [t for t in table_order if t in target_tables]

        # Sort records for self-referential relationships
        for rel in self._config.relationships:
            if isinstance(rel, BelongsToRelationship) and rel.table == rel.references:
                records = self._sort_records_for_self_ref(records, rel)
                break

        # Group fields by table for quick lookup.
        # Start with top-level fields, then layer in collection fields
        # for the target tables being inserted.
        table_fields: dict[str, list] = defaultdict(list)
        for field in self._config.schema_.fields:
            table_fields[field.db.table].append(field)

        # Include collection fields for tables in this insert batch.
        # Collection fields provide the column mappings for child tables.
        if self._config.schema_.collections:
            for collection in self._config.schema_.collections:
                for field in collection.fields:
                    existing = table_fields[field.db.table]
                    if not any(f.db.column == field.db.column for f in existing):
                        table_fields[field.db.table].append(field)

        # Find junction relationships
        junctions = [
            r for r in self._config.relationships if isinstance(r, JunctionRelationship)
        ]
        belongs_tos = [
            r for r in self._config.relationships if isinstance(r, BelongsToRelationship)
        ]

        # Empty input: return immediately without invoking the callback.
        if not records:
            logger.info("Inserted 0 records")
            return 0

        # Determine batch size — None or invalid means "all at once"
        if batch_size and batch_size > 0:
            effective_batch = batch_size
        else:
            effective_batch = len(records)

        inserted_count = 0
        for batch_start in range(0, len(records), effective_batch):
            batch = records[batch_start : batch_start + effective_batch]
            try:
                async with self._db.session() as session:
                    async with session.begin():
                        for record in batch:
                            await self._insert_one_record(
                                session,
                                record,
                                table_order,
                                table_fields,
                                junctions,
                                belongs_tos,
                                audit_logger=audit_logger,
                            )
                        # Transaction commits at end of `async with session.begin()`
            except DatabaseError:
                if audit_logger is not None:
                    audit_logger.clear()
                raise
            except Exception as e:
                if audit_logger is not None:
                    audit_logger.clear()
                raise DatabaseError(
                    f"Insertion failed, transaction rolled back: {e}"
                ) from e

            # Flush audit entries for this successfully committed batch
            if audit_logger is not None:
                await audit_logger.flush()

            inserted_count += len(batch)
            if on_batch is not None:
                res = on_batch(inserted_count)
                if asyncio.iscoroutine(res):
                    await res

        logger.info(f"Inserted {inserted_count} records")
        return inserted_count

    async def _insert_one_record(
        self,
        session,
        record: dict,
        table_order: list[str],
        table_fields: dict,
        junctions: list,
        belongs_tos: list,
        audit_logger: "AuditLogger | None" = None,
    ) -> None:
        """Insert a single record across all relevant tables and junctions.

        Mutates ``self._lookup_cache`` and uses the provided session. Caller
        is responsible for transaction boundaries.
        """
        # Track inserted IDs for this record per table
        record_ids: dict[str, any] = {}

        for table_name in table_order:
            model = self._models[table_name]
            pk_config = self._config.schema_.tables[table_name].primary_key

            # Build row data from record fields mapped to this table
            row_data = {}
            for field in table_fields.get(table_name, []):
                value = record.get(field.name)
                if value is not None:
                    row_data[field.db.column] = value

            # Generate UUID if needed
            if pk_config.type == "uuid":
                row_data[pk_config.column] = str(uuid.uuid4())

            # Resolve belongs_to FK values
            for rel in belongs_tos:
                if rel.table == table_name:
                    ref_value = record.get(rel.field)
                    if ref_value:
                        fk_value = self._lookup_cache.get(
                            rel.references, {}
                        ).get(ref_value)
                        if fk_value is not None:
                            row_data[rel.fk_column] = fk_value

            # Skip if no data columns for an auto_increment table
            if not row_data and pk_config.type == "auto_increment":
                continue

            table_cfg = self._config.schema_.tables[table_name]
            if table_cfg.on_conflict is None or table_cfg.on_conflict.action == "error":
                # No upsert configured (or action=error) — use ORM insert as before
                instance = model(**row_data)
                session.add(instance)
                await session.flush()
                pk_value = getattr(instance, pk_config.column)
                if audit_logger is not None:
                    audit_logger.record(AuditEntry(
                        target_table=table_name,
                        target_pk=str(pk_value),
                        action="insert",
                    ))
            else:
                # Upsert path
                pk_value, action, field_changes = await self._upsert_executor.execute(
                    session, model, row_data, pk_config, table_cfg.on_conflict,
                )
                # action is None for no-op upserts (existing row, no real change)
                if audit_logger is not None and action is not None:
                    audit_logger.record(AuditEntry(
                        target_table=table_name,
                        target_pk=str(pk_value),
                        action=action,
                        field_changes=field_changes,
                    ))

            record_ids[table_name] = pk_value

            # Update lookup cache for belongs_to resolution
            for rel in belongs_tos:
                if rel.references == table_name:
                    resolve_col = rel.resolve_by
                    resolve_val = row_data.get(resolve_col)
                    if resolve_val:
                        self._lookup_cache[table_name][resolve_val] = pk_value

        # Insert junction rows for this record
        for junc in junctions:
            t1, t2 = junc.link
            if t1 in record_ids and t2 in record_ids:
                junc_model = self._models[junc.through]
                junc_row = junc_model(
                    **{
                        junc.columns[t1]: record_ids[t1],
                        junc.columns[t2]: record_ids[t2],
                    }
                )
                session.add(junc_row)

    def generate_sql_preview(self, records: list[dict]) -> list[str]:
        """Generate a preview of SQL INSERT statements (for HITL review).

        Does NOT execute anything -- just builds the SQL strings.
        Previews at most 5 records.
        """
        table_order = self.topological_sort()
        table_fields: dict[str, list] = defaultdict(list)
        for field in self._config.schema_.fields:
            table_fields[field.db.table].append(field)

        statements = []
        for record in records[:5]:  # Preview first 5 records
            for table_name in table_order:
                columns = []
                values = []
                for field in table_fields.get(table_name, []):
                    value = record.get(field.name)
                    if value is not None:
                        columns.append(field.db.column)
                        values.append(repr(value))
                if columns:
                    stmt = (
                        f"INSERT INTO {table_name} "
                        f"({', '.join(columns)}) "
                        f"VALUES ({', '.join(values)})"
                    )
                    statements.append(stmt)

        return statements
