"""Relationship-aware record inserter with topological sort for the Siphon ETL pipeline."""

import logging
import uuid
from collections import defaultdict

from sqlalchemy import select

from siphon.config.schema import BelongsToRelationship, JunctionRelationship, SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.models import ModelGenerator
from siphon.db.upsert import (
    GenericUpsertPlan,
    build_upsert_statement,
    detect_dialect,
)
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
    - Single transaction with full rollback on failure
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

    def topological_sort(self) -> list[str]:
        """Sort table names so parents come before children (Kahn's algorithm).

        Only considers data tables (not junction tables).
        Tables with no dependencies come first.
        Raises DatabaseError if a circular dependency is detected.
        """
        data_tables = list(self._config.schema_.tables.keys())
        in_degree = {t: 0 for t in data_tables}
        graph = defaultdict(list)  # parent -> [children]

        for rel in self._config.relationships:
            if isinstance(rel, BelongsToRelationship):
                child = rel.table
                parent = rel.references
                if child != parent:  # skip self-referential for graph purposes
                    graph[parent].append(child)
                    in_degree[child] += 1

        # Kahn's algorithm
        queue = [t for t in data_tables if in_degree[t] == 0]
        result = []

        while queue:
            node = queue.pop(0)
            result.append(node)
            for neighbor in graph[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(result) != len(data_tables):
            raise DatabaseError("Circular dependency detected in table relationships")

        return result

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

    async def insert(self, records: list[dict], *, target_tables: set[str] | None = None) -> int:
        """Insert all records in a single transaction.

        Args:
            records: List of mapped record dicts.
            target_tables: If provided, only insert into these tables.
                          Used for collection records that should only go
                          into their specific child table(s).

        Returns number of records inserted.
        Raises DatabaseError on failure (entire batch rolled back).
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

        inserted_count = 0

        async with self._db.session() as session:
            try:
                async with session.begin():
                    for record in records:
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
                            else:
                                # Upsert path
                                pk_value = await self._execute_upsert(
                                    session, model, table_name, row_data, pk_config, table_cfg.on_conflict
                                )

                            record_ids[table_name] = pk_value

                            # Update lookup cache for belongs_to resolution
                            for rel in belongs_tos:
                                if rel.references == table_name:
                                    resolve_col = rel.resolve_by
                                    resolve_val = row_data.get(resolve_col)
                                    if resolve_val:
                                        self._lookup_cache[table_name][resolve_val] = pk_value

                        # Insert junction rows
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

                        inserted_count += 1

                    # Transaction commits at end of `async with session.begin()`

            except DatabaseError:
                raise
            except Exception as e:
                raise DatabaseError(
                    f"Insertion failed, transaction rolled back: {e}"
                ) from e

        logger.info(f"Inserted {inserted_count} records")
        return inserted_count

    async def _execute_upsert(
        self,
        session,
        model,
        table_name: str,
        row_data: dict,
        pk_config,
        on_conflict_cfg,
    ):
        """Execute an upsert statement and return the affected row's PK value."""
        db_conflict_key = self._field_names_to_columns(on_conflict_cfg.key)

        if on_conflict_cfg.update_columns == "all":
            db_update_columns = "all"
        else:
            db_update_columns = self._field_names_to_columns(on_conflict_cfg.update_columns)

        stmt = build_upsert_statement(
            dialect=self._dialect,
            table=model.__table__,
            row=row_data,
            conflict_key=db_conflict_key,
            action=on_conflict_cfg.action,
            update_columns=db_update_columns,
        )

        if isinstance(stmt, GenericUpsertPlan):
            return await self._execute_generic_upsert_plan(session, model, pk_config, stmt)

        await session.execute(stmt)
        return await self._lookup_pk_by_conflict_key(
            session, model, pk_config, db_conflict_key, row_data
        )

    def _field_names_to_columns(self, field_names: list[str]) -> list[str]:
        """Map schema field names to their DB column names."""
        name_to_column = {}
        for f in self._config.schema_.fields:
            name_to_column[f.name] = f.db.column
        if self._config.schema_.collections:
            for coll in self._config.schema_.collections:
                for f in coll.fields:
                    name_to_column[f.name] = f.db.column
        return [name_to_column.get(n, n) for n in field_names]

    async def _lookup_pk_by_conflict_key(
        self, session, model, pk_config, db_conflict_key, row_data
    ):
        """SELECT the PK after an upsert, matching on the conflict key."""
        from sqlalchemy import select
        pk_col = getattr(model, pk_config.column)
        stmt = select(pk_col)
        for col_name in db_conflict_key:
            stmt = stmt.where(getattr(model, col_name) == row_data[col_name])
        result = await session.execute(stmt)
        return result.scalar_one()

    async def _execute_generic_upsert_plan(self, session, model, pk_config, plan):
        """Execute a generic select-then-update plan."""
        from sqlalchemy import select, update as sa_update, insert as sa_insert

        pk_col = getattr(model, pk_config.column)
        select_stmt = select(pk_col)
        for col_name in plan.conflict_key:
            select_stmt = select_stmt.where(
                getattr(model, col_name) == plan.row[col_name]
            )
        existing = (await session.execute(select_stmt)).scalar_one_or_none()

        if existing is None:
            await session.execute(sa_insert(plan.table).values(**plan.row))
            return (await session.execute(select_stmt)).scalar_one()

        if plan.action == "skip":
            return existing

        if plan.update_columns == "all":
            update_values = {
                k: v for k, v in plan.row.items()
                if k not in plan.conflict_key
            }
        else:
            update_values = {
                k: plan.row[k] for k in plan.update_columns if k in plan.row
            }

        if update_values:
            update_stmt = sa_update(plan.table).values(**update_values)
            for col_name in plan.conflict_key:
                update_stmt = update_stmt.where(
                    getattr(model, col_name) == plan.row[col_name]
                )
            await session.execute(update_stmt)

        return existing

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
