# Siphon v3 Plan A: Code Refactor

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce structural complexity in two oversized methods (`pipeline.run()` 403 lines, `inserter.insert()` + `_insert_one_record()` 200+ lines) without changing behavior. All 1022 tests must remain green after every commit.

**Architecture:** This is a pure refactor. No new features, no test changes (other than tracking that everything still passes). The strategy is "extract method" — pull cohesive sub-tasks out of large methods into named helpers. New `UpsertExecutor` class moves upsert dispatch out of `Inserter`. New `siphon/utils/graph.py` module hosts the topological sort.

**Tech Stack:** No new dependencies. Existing Python 3.11+, SQLAlchemy 2.0 async, pytest.

**Audit findings driving this work:**
- `siphon/core/pipeline.py:run()` is 403 lines doing logging, source loading, mapping, joining, validation, dedup, dry-run handling, run tracking, audit setup, review, and insertion.
- `siphon/db/inserter.py:insert()` is 109 lines and `_insert_one_record()` is 97 lines.
- `topological_sort()` is a general graph algorithm embedded in a domain class.
- `run_tracker.py` and `audit.py` have function-local `import` statements that should be at module level.

**Branch:** Cut `v3-plan-a-code-refactor` from `v3-phase-6-multi-source-joins`.

---

## Strategy

This plan does refactors **smallest-to-largest** so each commit is verifiable independently:

1. **Tiny housekeeping first** (deferred imports, extract topological_sort) — proves the test suite and CI work as expected.
2. **Medium refactor** (split `_cross_validate`, simplify `_insert_one_record`).
3. **Big refactor** (split `pipeline.run()`).
4. **Architectural change** (extract `UpsertExecutor`).

If anything goes sideways, we've already banked the easy wins.

**TDD note:** This is structural refactoring, NOT TDD. The existing test suite IS the safety net. After each task we verify all 1022 tests still pass. We don't write new tests for the refactor itself — that would test implementation detail.

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/utils/graph.py` | Topological sort (general algorithm) | Create |
| `siphon/db/inserter.py` | Slim Inserter, delegating upserts | Modify |
| `siphon/db/upsert_executor.py` | New: encapsulates upsert SQL dispatch + diff computation | Create |
| `siphon/core/pipeline.py` | Slim `run()` orchestrating `_run_*` helpers | Modify |
| `siphon/db/run_tracker.py` | Module-level imports cleanup | Modify |
| `siphon/db/audit.py` | Module-level imports cleanup | Modify |
| `siphon/config/loader.py` | Split `_cross_validate` into smaller validators | Modify |

No test files are modified in this plan. The existing tests prove behavior is preserved.

---

## Task Breakdown

### Task 1: Branch + baseline

**Files:** branch only

- [ ] **Step 1: Cut a new branch**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v3-phase-6-multi-source-joins
git pull
git checkout -b v3-plan-a-code-refactor
```

- [ ] **Step 2: Verify the v3.5 baseline**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`. Lock this number — every later task must end with the same count.

- [ ] **Step 3: No commit yet** — branch creation alone doesn't need a commit.

---

### Task 2: Module-level imports cleanup

**Files:**
- Modify: `siphon/db/run_tracker.py`
- Modify: `siphon/db/audit.py`

The audit found `from sqlalchemy import update`, `select` and `import json` placed inside method bodies. Move them to module top.

- [ ] **Step 1: Move sqlalchemy imports to module level in `siphon/db/run_tracker.py`**

Find the `from sqlalchemy import update` and `from sqlalchemy import select` lines that appear inside method bodies (around lines 85, 97, 128, 143). Move them to the top of the file alongside the existing sqlalchemy imports. The top of the file should look like:

```python
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
    select,
    update,
)
from sqlalchemy.orm import DeclarativeBase

from siphon.db.engine import DatabaseEngine
```

Then delete the inline imports inside the method bodies. The method bodies should reference `select(...)` and `update(...)` directly without local imports.

- [ ] **Step 2: Move `import json` to module level in `siphon/db/audit.py`**

Find the `import json` line inside `flush()` (around line 116). Move it to the module top alongside other imports:

```python
"""Manages the _siphon_audit metadata table for per-record action logging."""

from __future__ import annotations

import json
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
```

Then remove the inline `import json` inside `flush()`.

- [ ] **Step 3: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`.

- [ ] **Step 4: Commit**

```bash
git add siphon/db/run_tracker.py siphon/db/audit.py
git commit -m "refactor: move deferred imports to module level"
```

---

### Task 3: Extract topological_sort to siphon/utils/graph.py

**Files:**
- Create: `siphon/utils/graph.py`
- Modify: `siphon/db/inserter.py`

The audit flagged `topological_sort()` (Kahn's algorithm) as a general graph algorithm embedded in `Inserter`. Move it to a utility module so it's reusable and the Inserter is leaner.

- [ ] **Step 1: Create `siphon/utils/graph.py`**

Create the file:

```python
"""General-purpose graph utilities."""

from __future__ import annotations

from collections import defaultdict


def topological_sort(
    nodes: list[str],
    edges: list[tuple[str, str]],
) -> list[str]:
    """Topologically sort nodes using Kahn's algorithm.

    Args:
        nodes: All node names to be sorted.
        edges: List of (parent, child) tuples — parent must come before child.
                Self-loops (parent == child) are silently ignored.

    Returns:
        A list of node names in topological order.

    Raises:
        ValueError: If the graph contains a cycle.
    """
    in_degree: dict[str, int] = {n: 0 for n in nodes}
    graph: dict[str, list[str]] = defaultdict(list)

    for parent, child in edges:
        if parent == child:
            continue
        graph[parent].append(child)
        in_degree[child] += 1

    queue = [n for n in nodes if in_degree[n] == 0]
    result: list[str] = []

    while queue:
        node = queue.pop(0)
        result.append(node)
        for neighbor in graph[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(result) != len(nodes):
        raise ValueError("Cycle detected in graph")

    return result
```

- [ ] **Step 2: Update `siphon/db/inserter.py` to use the utility**

Read the current `topological_sort()` method in `Inserter`. It builds nodes from `data_tables` and edges from `BelongsToRelationship` entries.

Replace the body with a call to the new utility. The `Inserter.topological_sort()` method becomes:

```python
def topological_sort(self) -> list[str]:
    """Sort table names so parents come before children.

    Only considers data tables (not junction tables). Self-referential
    relationships are excluded from the graph (handled separately by
    record-level sorting).

    Raises:
        DatabaseError: If a circular dependency is detected.
    """
    from siphon.utils.errors import DatabaseError
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
```

The local import of `DatabaseError` and `_topo_sort` keeps the diff small. If `DatabaseError` is already imported at module level, drop the local import.

- [ ] **Step 3: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`.

- [ ] **Step 4: Commit**

```bash
git add siphon/utils/graph.py siphon/db/inserter.py
git commit -m "refactor: extract topological_sort to siphon/utils/graph"
```

---

### Task 4: Split `_cross_validate` in config/loader.py

**Files:**
- Modify: `siphon/config/loader.py`

The audit found `_cross_validate()` does 4 unrelated validations (fields, on_conflict, joins, etc.) in 44 lines. Split into focused validators.

- [ ] **Step 1: Read the current `_cross_validate` in `siphon/config/loader.py`**

Identify the distinct validation concerns. Currently they are likely:
1. Field-type-specific validation (enum/regex/subdivision)
2. on_conflict.key validates against known field names
3. join.left/right validate against declared sources
4. Custom transform requires `transforms.file`

- [ ] **Step 2: Refactor to call sub-validators**

Replace `_cross_validate` with:

```python
def _cross_validate(config: SiphonConfig) -> None:
    """Apply cross-validation rules that depend on multiple fields.

    Raises ConfigError for any violation.
    """
    _validate_field_type_requirements(config)
    _validate_on_conflict_keys(config)
    _validate_join_source_references(config)
    _validate_custom_transform_requires_file(config)
```

Then implement each helper. Read the existing body of `_cross_validate` and extract each block of validation into its own helper. Each helper should do ONE thing and have a clear docstring.

Example structure (the EXACT bodies must match what's in the existing `_cross_validate` — extract them verbatim):

```python
def _validate_field_type_requirements(config: SiphonConfig) -> None:
    """Enforce that enum needs values/preset, regex needs pattern,
    subdivision needs country_code."""
    for field in config.schema_.fields or []:
        _validate_field(field, config, context="schema")
    if config.schema_.collections:
        for collection in config.schema_.collections:
            for field in collection.fields:
                _validate_field(
                    field, config,
                    context=f"collection '{collection.name}'",
                )


def _validate_on_conflict_keys(config: SiphonConfig) -> None:
    """Validate on_conflict.key references known field names."""
    known_field_names = {f.name for f in config.schema_.fields or []}
    if config.schema_.collections:
        for collection in config.schema_.collections:
            for field in collection.fields:
                known_field_names.add(field.name)

    for table_name, table_cfg in config.schema_.tables.items():
        if table_cfg.on_conflict is None:
            continue
        for key_field in table_cfg.on_conflict.key:
            if key_field not in known_field_names:
                raise ConfigError(
                    f"Table '{table_name}' on_conflict.key references unknown "
                    f"field '{key_field}'. Known fields: {sorted(known_field_names)}"
                )


def _validate_join_source_references(config: SiphonConfig) -> None:
    """Validate join.left and join.right reference real sources."""
    if not (config.sources and config.joins):
        return
    source_names = {s.name for s in config.sources}
    for i, j in enumerate(config.joins):
        if j.left not in source_names:
            raise ConfigError(
                f"join[{i}].left='{j.left}' is not a declared source. "
                f"Known sources: {sorted(source_names)}"
            )
        if j.right not in source_names:
            raise ConfigError(
                f"join[{i}].right='{j.right}' is not a declared source. "
                f"Known sources: {sorted(source_names)}"
            )


def _validate_custom_transform_requires_file(config: SiphonConfig) -> None:
    """If any field uses transform.type='custom', transforms.file must be set."""
    # Read the existing logic for this and copy it verbatim.
    # If there isn't a separate block for this in the current code, skip
    # creating this helper.
```

**IMPORTANT:** Read the existing `_cross_validate` carefully and extract EVERY validation it currently performs. Don't drop any. If the existing code has additional validations not enumerated above, give them their own helper.

- [ ] **Step 3: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`. Loader and config tests are particularly important — verify they all pass.

- [ ] **Step 4: Commit**

```bash
git add siphon/config/loader.py
git commit -m "refactor: split _cross_validate into focused helpers"
```

---

### Task 5: Extract UpsertExecutor class

**Files:**
- Create: `siphon/db/upsert_executor.py`
- Modify: `siphon/db/inserter.py`

The Inserter currently has `_execute_upsert`, `_lookup_existing_row`, `_compute_upsert_changes`, `_lookup_pk_by_conflict_key`, `_execute_generic_upsert_plan`, and `_field_names_to_columns` methods that all serve the upsert path. Move them into a dedicated `UpsertExecutor` class.

- [ ] **Step 1: Create `siphon/db/upsert_executor.py`**

```python
"""UpsertExecutor — encapsulates the upsert path with diff computation.

Used by the Inserter when on_conflict is configured on a table. Returns
(pk_value, action, field_changes) so the inserter can emit accurate audit
entries.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import insert as sa_insert, select, update as sa_update

from siphon.db.upsert import (
    GenericUpsertPlan,
    build_upsert_statement,
)


class UpsertExecutor:
    """Executes upsert statements and computes field-level diffs.

    Holds a reference to the dialect string and the field-name → column-name
    map. Stateless across calls — safe to reuse across many records.
    """

    def __init__(
        self,
        *,
        dialect: str,
        field_name_to_column: dict[str, str],
    ) -> None:
        self._dialect = dialect
        self._name_to_column = field_name_to_column

    def _names_to_columns(self, names: list[str]) -> list[str]:
        return [self._name_to_column.get(n, n) for n in names]

    async def execute(
        self,
        session,
        model,
        row_data: dict,
        pk_config,
        on_conflict_cfg,
    ) -> tuple[Any, str | None, dict | None]:
        """Run the upsert and return (pk_value, action, field_changes).

        action is one of:
        - "insert" — no existing row matched
        - "update" — existing row had different values; field_changes populated
        - "skip" — explicit on_conflict.action="skip" with an existing row
        - None — no-op (existing row matched but values were identical)

        field_changes is None unless action == "update".
        """
        db_conflict_key = self._names_to_columns(on_conflict_cfg.key)

        if on_conflict_cfg.update_columns == "all":
            db_update_columns = "all"
        else:
            db_update_columns = self._names_to_columns(on_conflict_cfg.update_columns)

        # Look up existing row BEFORE upsert so we can compute field changes
        existing_row = await self._lookup_existing_row(
            session, model, db_conflict_key, row_data,
        )

        stmt = build_upsert_statement(
            dialect=self._dialect,
            table=model.__table__,
            row=row_data,
            conflict_key=db_conflict_key,
            action=on_conflict_cfg.action,
            update_columns=db_update_columns,
        )

        if isinstance(stmt, GenericUpsertPlan):
            pk_value = await self._execute_generic_plan(session, model, pk_config, stmt)
        else:
            await session.execute(stmt)
            pk_value = await self._lookup_pk_by_conflict_key(
                session, model, pk_config, db_conflict_key, row_data,
            )

        # Determine action and field_changes
        if existing_row is None:
            return pk_value, "insert", None

        if on_conflict_cfg.action == "skip":
            return pk_value, "skip", None

        # action == "update": compute the diff
        field_changes = self._compute_changes(
            row_data, existing_row, db_conflict_key, db_update_columns, model,
        )

        if not field_changes:
            return pk_value, None, None  # no-op

        return pk_value, "update", field_changes

    async def _lookup_existing_row(
        self, session, model, db_conflict_key: list[str], row_data: dict,
    ) -> dict | None:
        stmt = select(model)
        for col_name in db_conflict_key:
            stmt = stmt.where(getattr(model, col_name) == row_data[col_name])
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return {col.name: getattr(row, col.name) for col in model.__table__.columns}

    async def _lookup_pk_by_conflict_key(
        self, session, model, pk_config, db_conflict_key: list[str], row_data: dict,
    ) -> Any:
        pk_col = getattr(model, pk_config.column)
        stmt = select(pk_col)
        for col_name in db_conflict_key:
            stmt = stmt.where(getattr(model, col_name) == row_data[col_name])
        result = await session.execute(stmt)
        return result.scalar_one()

    async def _execute_generic_plan(
        self, session, model, pk_config, plan: GenericUpsertPlan,
    ) -> Any:
        # Read the existing _execute_generic_upsert_plan from inserter.py and
        # copy its body here verbatim. Replace any references to self that
        # don't apply (e.g., self._lookup_pk_by_conflict_key now lives on
        # this same class).
        # ...
        # This body must match the existing logic exactly.
        raise NotImplementedError("Copy from inserter._execute_generic_upsert_plan")

    def _compute_changes(
        self,
        row_data: dict,
        existing_row: dict,
        key_columns: list[str],
        update_columns,
        model,
    ) -> dict[str, dict]:
        key_set = set(key_columns)

        if update_columns == "all":
            candidates = [
                col.name for col in model.__table__.columns
                if col.name not in key_set and col.name in row_data
            ]
        else:
            candidates = [c for c in update_columns if c in row_data]

        changes: dict[str, dict] = {}
        for col_name in candidates:
            new_val = row_data.get(col_name)
            old_val = existing_row.get(col_name)
            if new_val != old_val:
                changes[col_name] = {"old": old_val, "new": new_val}
        return changes
```

**IMPORTANT:** For `_execute_generic_plan`, READ the current `_execute_generic_upsert_plan` body in `siphon/db/inserter.py` and copy it verbatim. Do NOT skip this — the placeholder above is a marker, not real code. The body must be identical to what's in the inserter today.

- [ ] **Step 2: Update `siphon/db/inserter.py` to use `UpsertExecutor`**

In `Inserter.__init__`, build the field-name→column map once and instantiate the executor:

```python
def __init__(self, config, db_engine, model_generator):
    self._config = config
    self._db = db_engine
    self._models = model_generator.models
    self._generator = model_generator
    self._lookup_cache = defaultdict(dict)
    self._dialect = detect_dialect(config.database.url)

    # Build the field-name → DB column-name map
    name_to_column: dict[str, str] = {}
    for f in self._config.schema_.fields or []:
        name_to_column[f.name] = f.db.column if f.db else f.name
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
```

In `_insert_one_record`, replace the call to `self._execute_upsert(...)` with:

```python
                pk_value, action, field_changes = await self._upsert_executor.execute(
                    session, model, row_data, pk_config, table_cfg.on_conflict,
                )
```

The audit emission block stays the same.

- [ ] **Step 3: Delete the now-unused methods from Inserter**

Delete these methods from `siphon/db/inserter.py` (they live in `UpsertExecutor` now):
- `_execute_upsert`
- `_field_names_to_columns`
- `_lookup_existing_row`
- `_lookup_pk_by_conflict_key`
- `_execute_generic_upsert_plan`
- `_compute_upsert_changes`

If any other Inserter method calls these, update those callers to use `self._upsert_executor` instead.

- [ ] **Step 4: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`. The upsert tests in particular (`test_inserter_upsert.py`, `test_inserter_audit.py`) are the proof.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/upsert_executor.py siphon/db/inserter.py
git commit -m "refactor: extract UpsertExecutor class from Inserter"
```

---

### Task 6: Slim down `Inserter._insert_one_record`

**Files:**
- Modify: `siphon/db/inserter.py`

After Task 5, `_insert_one_record` is still ~70 lines. Extract two helpers:
1. `_build_row_data_for_table()` — build the row dict (field mapping + UUID generation + FK resolution)
2. `_emit_audit_for_record()` — record the audit entry

- [ ] **Step 1: Read the current `_insert_one_record`**

It currently does, for each table in topological order:
1. Build row data from record fields mapped to this table
2. Generate UUID if PK is uuid type
3. Resolve belongs_to FK values from lookup cache
4. Insert via ORM or upsert
5. Update lookup cache
6. Emit audit entry

Then for junctions: insert a junction row with both PKs.

- [ ] **Step 2: Extract `_build_row_data_for_table`**

Add this method to `Inserter`:

```python
def _build_row_data_for_table(
    self,
    record: dict,
    table_name: str,
    pk_config,
    table_fields: dict,
    belongs_tos: list,
) -> dict:
    """Build the row dict for a single table from a single mapped record.

    Includes:
    - Field values mapped to this table.
    - A generated UUID if the PK type is uuid.
    - Resolved FK values for belongs_to relationships pointing to this table.
    """
    row_data: dict = {}

    # 1. Field values mapped to this table
    for field in table_fields.get(table_name, []):
        value = record.get(field.name)
        if value is not None:
            row_data[field.db.column] = value

    # 2. Generate UUID if needed
    if pk_config.type == "uuid":
        row_data[pk_config.column] = str(uuid.uuid4())

    # 3. Resolve belongs_to FK values
    for rel in belongs_tos:
        if rel.table == table_name:
            ref_value = record.get(rel.field)
            if ref_value:
                fk_value = self._lookup_cache.get(rel.references, {}).get(ref_value)
                if fk_value is not None:
                    row_data[rel.fk_column] = fk_value

    return row_data
```

- [ ] **Step 3: Use the helper in `_insert_one_record`**

Replace the per-table inner loop's "build row data" section with a single call:

```python
    for table_name in table_order:
        model = self._models[table_name]
        pk_config = self._config.schema_.tables[table_name].primary_key

        row_data = self._build_row_data_for_table(
            record, table_name, pk_config, table_fields, belongs_tos,
        )

        if not row_data and pk_config.type == "auto_increment":
            continue  # nothing to insert

        # ... rest of the loop (insert vs upsert dispatch, audit emission) ...
```

- [ ] **Step 4: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/inserter.py
git commit -m "refactor: extract _build_row_data_for_table from _insert_one_record"
```

---

### Task 7: Extract `_insert_batch` from `Inserter.insert`

**Files:**
- Modify: `siphon/db/inserter.py`

`Inserter.insert()` is still ~100 lines after Tasks 5–6. Extract the per-batch transaction handling into `_insert_batch`.

- [ ] **Step 1: Add `_insert_batch` to `Inserter`**

```python
async def _insert_batch(
    self,
    batch: list[dict],
    *,
    table_order: list[str],
    table_fields: dict,
    junctions: list,
    belongs_tos: list,
    audit_logger,
) -> None:
    """Insert a single batch of records inside one transaction.

    Audit entries collected during the batch are flushed if the transaction
    commits successfully; cleared if it fails.
    """
    try:
        async with self._db.session() as session:
            async with session.begin():
                for record in batch:
                    await self._insert_one_record(
                        session, record, table_order, table_fields,
                        junctions, belongs_tos,
                        audit_logger=audit_logger,
                    )
    except Exception:
        if audit_logger is not None:
            audit_logger.clear()
        raise

    if audit_logger is not None:
        await audit_logger.flush()
```

- [ ] **Step 2: Refactor `Inserter.insert` to use `_insert_batch`**

Replace the per-batch loop body inside `insert()` with a call to `_insert_batch`. The simplified `insert()` should look roughly like:

```python
async def insert(
    self,
    records: list[dict],
    *,
    target_tables: set[str] | None = None,
    batch_size: int | None = None,
    on_batch=None,
    audit_logger=None,
) -> int:
    """[existing docstring]"""

    table_order = self.topological_sort()
    if target_tables is not None:
        table_order = [t for t in table_order if t in target_tables]

    # Sort records for self-referential relationships
    for rel in self._config.relationships:
        if isinstance(rel, BelongsToRelationship) and rel.table == rel.references:
            records = self._sort_records_for_self_ref(records, rel)
            break

    table_fields = self._build_table_fields_map()
    junctions = [r for r in self._config.relationships if isinstance(r, JunctionRelationship)]
    belongs_tos = [r for r in self._config.relationships if isinstance(r, BelongsToRelationship)]

    effective_batch = batch_size if batch_size and batch_size > 0 else max(len(records), 1)

    inserted_count = 0
    for batch_start in range(0, len(records), effective_batch):
        batch = records[batch_start : batch_start + effective_batch]
        try:
            await self._insert_batch(
                batch,
                table_order=table_order,
                table_fields=table_fields,
                junctions=junctions,
                belongs_tos=belongs_tos,
                audit_logger=audit_logger,
            )
        except Exception as e:
            from siphon.utils.errors import DatabaseError
            raise DatabaseError(
                f"Insert failed at record {inserted_count + 1}: {e}"
            ) from e

        inserted_count += len(batch)
        if on_batch is not None:
            res = on_batch(inserted_count)
            if asyncio.iscoroutine(res):
                await res

    if not records:
        return 0
    return inserted_count
```

- [ ] **Step 3: Add `_build_table_fields_map`**

The current `insert()` has inline code building `table_fields`. Extract it:

```python
def _build_table_fields_map(self) -> dict[str, list]:
    """Map each target table name to its list of FieldConfigs."""
    table_fields: dict[str, list] = defaultdict(list)
    for field in self._config.schema_.fields or []:
        if field.db:
            table_fields[field.db.table].append(field)

    if self._config.schema_.collections:
        for collection in self._config.schema_.collections:
            for field in collection.fields:
                if field.db:
                    existing = table_fields[field.db.table]
                    if not any(f.db.column == field.db.column for f in existing):
                        table_fields[field.db.table].append(field)

    return table_fields
```

- [ ] **Step 4: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/inserter.py
git commit -m "refactor: extract _insert_batch and _build_table_fields_map"
```

---

### Task 8: Split `Pipeline.run` into orchestration helpers

**Files:**
- Modify: `siphon/core/pipeline.py`

The biggest refactor. `Pipeline.run` is 403 lines. Goal: reduce to ~150 lines of orchestration, with stages extracted into clearly-named helpers.

**Strategy:** Extract three helpers in this order, each preserving behavior:
1. `_load_and_map_records()` — handles single-source vs multi-source loading + mapping + joining. Returns a tuple `(records, all_collection_records, effective_input_path)`.
2. `_setup_run_tracking_and_audit()` — creates the RunTracker and AuditLogger, handles resume detection. Returns `(run_tracker, run_id, audit_logger, records_to_insert)`.
3. `_handle_dry_run()` — computes diff and stores it on result. Returns nothing (mutates result).

After extraction, `run()` becomes a sequence of stage calls.

- [ ] **Step 1: Extract `_load_and_map_records`**

Read the current `run()` and identify the section that:
- Loads custom transforms
- Branches on `self._config.sources` vs `self._config.source`
- Loads each source and maps it
- Applies joins
- Sets `records`, `all_collection_records`, `effective_input_path`

Move this into a new method on `Pipeline`. The signature:

```python
async def _load_and_map_records(
    self, input_path: Path, sheet: str | int | None,
) -> tuple[list[dict], dict[str, list[dict]], str]:
    """Load source data, map to target schema, apply joins.

    Returns (records, all_collection_records, effective_input_path).
    """
    # COPY THE EXISTING multi-source-vs-single-source logic VERBATIM here.
    # Return the three values at the end.
```

In `run()`, replace the inline section with:

```python
        records, all_collection_records, effective_input_path = (
            await self._load_and_map_records(input_path, sheet)
        )
        result.total_extracted = len(records)
```

**IMPORTANT:** Read the existing logic carefully. It's complex (multi-source mode mutates `self._config.schema_.fields`, single-source mode handles XML collections via `mapper.map_collections`). Both paths must continue working.

- [ ] **Step 2: Run the full suite after the first extraction**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`. If anything fails here, revert and try smaller extractions.

- [ ] **Step 3: Commit (intermediate)**

```bash
git add siphon/core/pipeline.py
git commit -m "refactor: extract _load_and_map_records from Pipeline.run"
```

- [ ] **Step 4: Extract `_handle_dry_run`**

Find the `if dry_run:` block (currently sets up Differ, calls compute_diff, stores in result.diff, returns). Move into:

```python
async def _handle_dry_run(
    self,
    result: "PipelineResult",
    valid_records: list[dict],
) -> None:
    """Compute the dry-run diff and store on result. Mutates result."""
    db_engine = DatabaseEngine(self._config.database)
    try:
        model_gen = ModelGenerator(self._config)
        model_gen.generate()
        differ = Differ(self._config, db_engine, model_gen)
        try:
            result.diff = await differ.compute_diff(valid_records)
        except Exception as e:
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
```

In `run()`, replace the dry_run block with:

```python
        if dry_run:
            await self._handle_dry_run(result, valid_records)
            return result
```

- [ ] **Step 5: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`.

- [ ] **Step 6: Commit (intermediate)**

```bash
git add siphon/core/pipeline.py
git commit -m "refactor: extract _handle_dry_run from Pipeline.run"
```

- [ ] **Step 7: Extract `_setup_run_tracking_and_audit`**

Find the run-tracking/audit-logger section (currently uses `RunTracker.create_runs_table`, optionally calls `find_resumable_run`, then `start_run`, then maybe creates `AuditLogger`). Move into:

```python
async def _setup_run_tracking_and_audit(
    self,
    db_engine: "DatabaseEngine",
    valid_records: list[dict],
    effective_input_path: str,
    *,
    resume: bool,
    user: str | None,
) -> tuple["RunTracker | None", int | None, "AuditLogger | None", list[dict]]:
    """Set up run tracking + audit logging for an upcoming insert.

    If `resume=True` and a resumable failed run exists, slice records_to_insert
    to skip already-processed records.

    Returns (run_tracker, run_id, audit_logger, records_to_insert).
    """
    # COPY the existing logic verbatim. The shape is:
    # 1. If track_runs: create RunTracker, create_runs_table.
    # 2. If resume: find_resumable_run, slice records.
    # 3. start_run, capture run_id.
    # 4. If audit: create AuditLogger, create_audit_table.
    # 5. Return all four values.
```

In `run()`, the insert section becomes:

```python
            run_tracker, run_id, audit_logger, records_to_insert = (
                await self._setup_run_tracking_and_audit(
                    db_engine,
                    valid_records,
                    effective_input_path,
                    resume=resume,
                    user=user,
                )
            )

            async def _on_batch(committed: int) -> None:
                if run_tracker is not None and run_id is not None:
                    skipped = len(valid_records) - len(records_to_insert)
                    await run_tracker.update_progress(run_id, skipped + committed)

            try:
                result.total_inserted = await inserter.insert(
                    records_to_insert,
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
```

- [ ] **Step 8: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`. The resume tests (`test_pipeline_resume.py`, `test_integration_resume.py`) and audit tests (`test_pipeline_audit.py`, `test_integration_audit.py`) are the proof.

- [ ] **Step 9: Commit**

```bash
git add siphon/core/pipeline.py
git commit -m "refactor: extract _setup_run_tracking_and_audit from Pipeline.run"
```

---

### Task 9: Final verification + line count check

**Files:** none — verification only

- [ ] **Step 1: Confirm test count is unchanged**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `1022 passed`.

- [ ] **Step 2: Verify the line counts dropped**

```bash
wc -l siphon/core/pipeline.py siphon/db/inserter.py
```

Expected:
- `pipeline.py` ~250 lines or less (was 535)
- `inserter.py` ~350 lines or less (was 557)

If `pipeline.py` is still over 350 lines, you've missed an extraction. Re-read `run()` and find another sub-stage to extract.

- [ ] **Step 3: Verify file structure**

```bash
ls siphon/utils/graph.py siphon/db/upsert_executor.py
```

Both should exist.

- [ ] **Step 4: No commit** — this is verification.

---

### Task 10: Push branch

**Files:** none — git operation only

- [ ] **Step 1: Push to origin**

```bash
git push -u origin v3-plan-a-code-refactor
```

- [ ] **Step 2: Confirm pushed**

The push output should show `Branch 'v3-plan-a-code-refactor' set up to track ...`.

---

## Verification

After all tasks complete:
1. `1022 passed` on `pytest tests/ -q`.
2. `wc -l siphon/core/pipeline.py siphon/db/inserter.py` shows substantial reductions.
3. `siphon/utils/graph.py` and `siphon/db/upsert_executor.py` exist.
4. Branch `v3-plan-a-code-refactor` pushed to origin.
5. Spot-check: `git log --oneline v3-phase-6-multi-source-joins..HEAD` shows ~8 small refactor commits, each focused.

## Self-Review Notes

**Behavior preservation:** Every commit must end with `1022 passed`. If any commit drops the count, the refactor is wrong — revert and try smaller steps.

**Type consistency:**
- `topological_sort(nodes, edges) -> list[str]` — same shape across Task 3 and Inserter callers.
- `UpsertExecutor.execute(...) -> tuple[Any, str | None, dict | None]` — same shape across Tasks 5 and 6 (the inserter unpacks this 3-tuple).
- `_insert_batch(batch, *, ...) -> None` — same shape in Task 7.
- `_load_and_map_records` returns `(records, all_collection_records, effective_input_path)` — used in Task 8 step 1 and consumed by `run()`.
- `_setup_run_tracking_and_audit(...) -> tuple[RunTracker | None, int | None, AuditLogger | None, list[dict]]` — Task 8 step 7.

**Placeholder scan:** The plan has one placeholder marker in Task 5 step 1 (`raise NotImplementedError("Copy from inserter._execute_generic_upsert_plan")`). This is intentional — the implementer MUST read the existing `inserter._execute_generic_upsert_plan` body and copy it verbatim. The plan flags this explicitly because that body is too long to inline reliably.

## Out of Scope

- **Test changes** — Plan B handles deletions and additions. Plan A only verifies the existing tests still pass.
- **New features** — None.
- **Performance** — Plan C handles bulk insert.
- **Documentation** — Plan C.
- **`models.py` `_build_data_tables` refactor** — flagged in audit (95 lines, multiple concerns) but lower priority than pipeline/inserter. Defer unless this plan finishes early.
