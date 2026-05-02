# Siphon v3 Phase 2: Dry-Run with Diff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `--dry-run` show what records *would* be inserted/updated/skipped/unchanged before any DB write happens.

**Architecture:** A new `Differ` class queries existing rows via the `on_conflict.key` from each table and categorizes each mapped record. Categories: `insert`, `update`, `skip`, `no_change`. The `PipelineResult` gains a `diff` field that the CLI renders as a Rich table (default) or JSON (`--output json`). When `on_conflict` is not configured for a table, every record is categorized as `insert`.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.0 async, Pydantic 2.0, Rich (CLI rendering), pytest, aiosqlite (test).

**Spec:** `docs/superpowers/specs/2026-04-22-siphon-v3-roadmap.md` (Phase 2 section)

**Branch:** Cut `v3-phase-2-dry-run-diff` from `v3-phase-1-upserts`.

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/db/differ.py` | Query existing rows + categorize records vs DB state | Create |
| `siphon/core/pipeline.py` | Compute diff in dry_run mode, store in PipelineResult | Modify |
| `siphon/cli.py` | Render diff (Rich + JSON), add `--output` flag | Modify |
| `tests/db/test_differ.py` | Unit tests for the Differ | Create |
| `tests/core/test_pipeline_diff.py` | Pipeline integration tests for dry-run diff | Create |
| `tests/test_cli_diff.py` | CLI tests for diff rendering and `--output json` | Create |
| `tests/test_integration_diff.py` | End-to-end test with two-CSV scenario | Create |

The `Differ` is a pure analysis component — it never writes. The pipeline calls it only when `dry_run=True`. Existing `dry_run` behavior (no insertion) is preserved; this just adds richer reporting.

---

## Data Shape

The `PipelineResult.diff` field is a dict:

```python
{
    "insert": [<record dicts>],
    "update": [{"key": {...}, "changes": {"col": {"old": ..., "new": ...}, ...}, "record": {...}}, ...],
    "skip": [<record dicts>],         # populated when on_conflict.action == "skip"
    "no_change": [<record dicts>],    # all values already match DB
}
```

Counts come from `len(...)` on each list — no separate count fields.

When a table has no `on_conflict` configured, every record for that table is categorized as `insert`.

---

## Task Breakdown

### Task 1: Branch + scaffold

**Files:** branch only

- [ ] **Step 1: Cut a new branch**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v3-phase-1-upserts
git pull
git checkout -b v3-phase-2-dry-run-diff
```

- [ ] **Step 2: Verify the v3.1 baseline**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `904 passed`.

- [ ] **Step 3: No commit yet** — branch creation alone doesn't need a commit.

---

### Task 2: Differ skeleton

**Files:**
- Create: `siphon/db/differ.py`
- Create: `tests/db/test_differ.py`

Establish the public interface and an empty implementation that returns "no changes" categories.

- [ ] **Step 1: Write the failing test**

Create `tests/db/test_differ.py`:

```python
"""Tests for the Differ — dry-run diff against existing DB state."""

from __future__ import annotations

import pytest
from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.differ import Differ
from siphon.db.models import ModelGenerator


def _make_config(on_conflict: dict | None = None) -> SiphonConfig:
    """Build a minimal config with one table, optionally with on_conflict."""
    table_cfg = {"primary_key": {"column": "id", "type": "auto_increment"}}
    if on_conflict is not None:
        table_cfg["on_conflict"] = on_conflict
    return SiphonConfig.model_validate({
        "name": "diff-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "companies", "column": "phone"}},
            ],
            "tables": {"companies": table_cfg},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def diff_setup():
    config = _make_config({"key": ["name"], "action": "update"})
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestDifferConstruction:
    async def test_differ_can_be_constructed(self, diff_setup):
        config, engine, model_gen = diff_setup
        differ = Differ(config, engine, model_gen)
        assert differ is not None

    async def test_compute_diff_returns_dict_with_categories(self, diff_setup):
        config, engine, model_gen = diff_setup
        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([])
        assert "insert" in result
        assert "update" in result
        assert "skip" in result
        assert "no_change" in result
        assert result["insert"] == []
        assert result["update"] == []
        assert result["skip"] == []
        assert result["no_change"] == []
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/db/test_differ.py -v
```

Expected: ImportError on `siphon.db.differ`.

- [ ] **Step 3: Create the module**

Create `siphon/db/differ.py`:

```python
"""Compute a dry-run diff between mapped records and current DB state."""

from __future__ import annotations

import logging
from typing import Any

from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.models import ModelGenerator

logger = logging.getLogger("siphon")


class Differ:
    """Categorize mapped records against existing DB state.

    For each table:
    - If no on_conflict configured: every record → "insert"
    - If on_conflict.key matches an existing row:
      - action="skip": record → "skip"
      - action="update", values match existing: record → "no_change"
      - action="update", values differ: record → "update" with field-level changes
      - action="error": record → "insert" (DB will reject at insert time)
    - If no existing row: record → "insert"
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

    async def compute_diff(self, records: list[dict]) -> dict[str, list]:
        """Compute the per-record diff against the current DB state."""
        return {
            "insert": [],
            "update": [],
            "skip": [],
            "no_change": [],
        }
```

- [ ] **Step 4: Verify tests pass**

```bash
.venv/bin/pytest tests/db/test_differ.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/differ.py tests/db/test_differ.py
git commit -m "feat: Differ skeleton with empty diff categories"
```

---

### Task 3: Differ — categorize as insert when no on_conflict

**Files:**
- Modify: `siphon/db/differ.py`
- Modify: `tests/db/test_differ.py`

When a table has no `on_conflict`, every record is an insert.

- [ ] **Step 1: Add the failing test**

Append to `tests/db/test_differ.py`:

```python
@pytest.fixture
async def diff_setup_no_conflict():
    """Setup without on_conflict — every record is an insert."""
    config = _make_config()  # no on_conflict
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestNoOnConflict:
    async def test_records_categorized_as_insert(self, diff_setup_no_conflict):
        config, engine, model_gen = diff_setup_no_conflict
        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([
            {"name": "Acme", "phone": "111"},
            {"name": "Beta", "phone": "222"},
        ])
        assert len(result["insert"]) == 2
        assert result["update"] == []
        assert result["skip"] == []
        assert result["no_change"] == []
```

- [ ] **Step 2: Verify the test fails**

```bash
.venv/bin/pytest tests/db/test_differ.py::TestNoOnConflict -v
```

Expected: FAIL — `result["insert"]` is empty.

- [ ] **Step 3: Implement**

Replace `compute_diff` in `siphon/db/differ.py` with:

```python
    async def compute_diff(self, records: list[dict]) -> dict[str, list]:
        """Compute the per-record diff against the current DB state."""
        result = {
            "insert": [],
            "update": [],
            "skip": [],
            "no_change": [],
        }

        # Group records by target table. A field's `db.table` tells us where it goes.
        # For now, assume every record goes to a single table — the first table that
        # has at least one field referencing it.
        target_table = self._infer_primary_table()

        if target_table is None:
            return result

        table_cfg = self._config.schema_.tables[target_table]

        if table_cfg.on_conflict is None:
            # Every record is an insert
            result["insert"] = list(records)
            return result

        return result

    def _infer_primary_table(self) -> str | None:
        """Return the first table that has at least one mapped field."""
        for field in self._config.schema_.fields:
            return field.db.table
        return None
```

- [ ] **Step 4: Verify tests pass**

```bash
.venv/bin/pytest tests/db/test_differ.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/differ.py tests/db/test_differ.py
git commit -m "feat: Differ — categorize as insert when no on_conflict"
```

---

### Task 4: Differ — categorize update vs no_change for existing rows

**Files:**
- Modify: `siphon/db/differ.py`
- Modify: `tests/db/test_differ.py`

When `on_conflict` is set and a row exists with the same key, compare values to decide if it's an update or no_change.

- [ ] **Step 1: Add the failing tests**

Append to `tests/db/test_differ.py`:

```python
class TestUpdateVsNoChange:
    async def test_existing_row_with_changed_values_is_update(self, diff_setup):
        """An existing row whose values differ should be categorized as 'update'."""
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup

        # Pre-populate the DB
        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([{"name": "Acme", "phone": "OLD"}])

        # Diff with new value
        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([{"name": "Acme", "phone": "NEW"}])

        assert len(result["update"]) == 1
        assert result["update"][0]["key"] == {"name": "Acme"}
        assert result["update"][0]["changes"] == {"phone": {"old": "OLD", "new": "NEW"}}
        assert result["update"][0]["record"] == {"name": "Acme", "phone": "NEW"}
        assert result["insert"] == []
        assert result["no_change"] == []

    async def test_existing_row_with_same_values_is_no_change(self, diff_setup):
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup

        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([{"name": "Acme", "phone": "111"}])

        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([{"name": "Acme", "phone": "111"}])

        assert len(result["no_change"]) == 1
        assert result["update"] == []
        assert result["insert"] == []

    async def test_new_row_is_insert(self, diff_setup):
        """A row whose key isn't in the DB should be 'insert' even with on_conflict set."""
        differ = Differ(*diff_setup)
        result = await differ.compute_diff([{"name": "BrandNew", "phone": "999"}])
        assert len(result["insert"]) == 1
        assert result["update"] == []
        assert result["no_change"] == []

    async def test_multiple_records_categorized_correctly(self, diff_setup):
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup

        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([
            {"name": "Existing-Same", "phone": "111"},
            {"name": "Existing-Changed", "phone": "OLD"},
        ])

        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([
            {"name": "Existing-Same", "phone": "111"},
            {"name": "Existing-Changed", "phone": "NEW"},
            {"name": "Brand-New", "phone": "222"},
        ])

        assert len(result["insert"]) == 1
        assert result["insert"][0]["name"] == "Brand-New"
        assert len(result["update"]) == 1
        assert result["update"][0]["record"]["name"] == "Existing-Changed"
        assert len(result["no_change"]) == 1
        assert result["no_change"][0]["name"] == "Existing-Same"
```

- [ ] **Step 2: Verify the tests fail**

```bash
.venv/bin/pytest tests/db/test_differ.py::TestUpdateVsNoChange -v
```

Expected: FAIL — currently records aren't being checked against the DB.

- [ ] **Step 3: Implement**

Replace `compute_diff` in `siphon/db/differ.py`:

```python
    async def compute_diff(self, records: list[dict]) -> dict[str, list]:
        """Compute the per-record diff against the current DB state."""
        result = {
            "insert": [],
            "update": [],
            "skip": [],
            "no_change": [],
        }

        target_table = self._infer_primary_table()
        if target_table is None or not records:
            if records:
                result["insert"] = list(records)
            return result

        table_cfg = self._config.schema_.tables[target_table]

        if table_cfg.on_conflict is None:
            result["insert"] = list(records)
            return result

        on_conflict = table_cfg.on_conflict
        # Map field names to DB column names
        field_to_column = self._field_to_column_map()
        key_columns = [field_to_column[name] for name in on_conflict.key]

        # Load existing rows keyed by the conflict key
        existing = await self._load_existing_rows(target_table, key_columns)

        for record in records:
            # Build a key tuple from this record's values
            key_values = tuple(self._record_value(record, field_to_column, name)
                               for name in on_conflict.key)
            existing_row = existing.get(key_values)

            if existing_row is None:
                result["insert"].append(record)
                continue

            if on_conflict.action == "skip":
                result["skip"].append(record)
                continue

            if on_conflict.action == "error":
                # Report as insert; the DB will reject at insert time.
                result["insert"].append(record)
                continue

            # action == "update": compare values
            changes = self._compute_changes(
                record, existing_row, field_to_column, on_conflict
            )

            if changes:
                key_dict = {name: record.get(name) for name in on_conflict.key}
                result["update"].append({
                    "key": key_dict,
                    "changes": changes,
                    "record": record,
                })
            else:
                result["no_change"].append(record)

        return result

    def _field_to_column_map(self) -> dict[str, str]:
        """Map schema field names to their DB column names."""
        mapping = {}
        for f in self._config.schema_.fields:
            mapping[f.name] = f.db.column
        if self._config.schema_.collections:
            for coll in self._config.schema_.collections:
                for f in coll.fields:
                    mapping[f.name] = f.db.column
        return mapping

    @staticmethod
    def _record_value(
        record: dict, field_to_column: dict[str, str], field_name: str
    ) -> Any:
        """Extract a value from a record, supporting both field-name and column-name keys."""
        if field_name in record:
            return record[field_name]
        col_name = field_to_column.get(field_name)
        if col_name and col_name in record:
            return record[col_name]
        return None

    async def _load_existing_rows(
        self, table_name: str, key_columns: list[str]
    ) -> dict[tuple, dict]:
        """Load all existing rows from a table, keyed by the conflict-key tuple."""
        from sqlalchemy import select

        model = self._models[table_name]
        async with self._db.session() as session:
            result = await session.execute(select(model))
            existing: dict[tuple, dict] = {}
            for row in result.scalars():
                key = tuple(getattr(row, col) for col in key_columns)
                row_dict = {col.name: getattr(row, col.name)
                            for col in model.__table__.columns}
                existing[key] = row_dict
        return existing

    def _compute_changes(
        self,
        record: dict,
        existing_row: dict,
        field_to_column: dict[str, str],
        on_conflict,
    ) -> dict[str, dict]:
        """Build a {column: {old, new}} dict for fields that differ.

        Only considers columns covered by `update_columns` (or all non-key
        columns if `update_columns == "all"`).
        """
        key_columns = {field_to_column[name] for name in on_conflict.key}

        if on_conflict.update_columns == "all":
            candidate_fields = [
                f.name for f in self._config.schema_.fields
                if f.db.table == self._infer_primary_table()
                and f.db.column not in key_columns
            ]
        else:
            candidate_fields = list(on_conflict.update_columns)

        changes = {}
        for field_name in candidate_fields:
            col_name = field_to_column.get(field_name, field_name)
            new_value = self._record_value(record, field_to_column, field_name)
            old_value = existing_row.get(col_name)
            if new_value != old_value:
                changes[col_name] = {"old": old_value, "new": new_value}
        return changes
```

- [ ] **Step 4: Verify tests pass**

```bash
.venv/bin/pytest tests/db/test_differ.py -v
```

Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/differ.py tests/db/test_differ.py
git commit -m "feat: Differ — compare existing rows for update vs no_change"
```

---

### Task 5: Differ — categorize as skip for action: skip

**Files:**
- Modify: `tests/db/test_differ.py`

The skip categorization is already handled by Task 4's implementation (the `if on_conflict.action == "skip":` branch). This task just adds tests to lock it in.

- [ ] **Step 1: Add the test**

Append to `tests/db/test_differ.py`:

```python
@pytest.fixture
async def diff_setup_skip():
    """Setup with action=skip."""
    config = _make_config({"key": ["name"], "action": "skip"})
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionSkipCategory:
    async def test_existing_row_categorized_as_skip(self, diff_setup_skip):
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup_skip

        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([{"name": "Acme", "phone": "OLD"}])

        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([{"name": "Acme", "phone": "NEW"}])

        assert len(result["skip"]) == 1
        assert result["update"] == []
        assert result["no_change"] == []

    async def test_new_row_with_action_skip_is_insert(self, diff_setup_skip):
        differ = Differ(*diff_setup_skip)
        result = await differ.compute_diff([{"name": "Acme", "phone": "111"}])
        assert len(result["insert"]) == 1
        assert result["skip"] == []
```

- [ ] **Step 2: Run tests**

```bash
.venv/bin/pytest tests/db/test_differ.py::TestActionSkipCategory -v
```

Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/db/test_differ.py
git commit -m "test: lock in skip categorization in Differ"
```

---

### Task 6: PipelineResult.diff field

**Files:**
- Modify: `siphon/core/pipeline.py`
- Modify: `tests/core/test_pipeline.py`

Add an optional `diff` field to `PipelineResult` so dry-run runs can return the categorized records.

- [ ] **Step 1: Write the failing test**

Append to `tests/core/test_pipeline.py`:

```python
class TestPipelineResultDiffField:
    def test_default_diff_is_none(self):
        from siphon.core.pipeline import PipelineResult
        result = PipelineResult()
        assert result.diff is None

    def test_diff_field_stores_dict(self):
        from siphon.core.pipeline import PipelineResult
        result = PipelineResult(diff={
            "insert": [], "update": [], "skip": [], "no_change": []
        })
        assert result.diff == {
            "insert": [], "update": [], "skip": [], "no_change": []
        }
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/core/test_pipeline.py::TestPipelineResultDiffField -v
```

Expected: FAIL — `PipelineResult` has no `diff` field.

- [ ] **Step 3: Add the field**

In `siphon/core/pipeline.py`, find the `PipelineResult` dataclass and add a `diff` field:

```python
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
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/core/test_pipeline.py::TestPipelineResultDiffField -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/core/pipeline.py tests/core/test_pipeline.py
git commit -m "feat: add diff field to PipelineResult"
```

---

### Task 7: Pipeline computes diff in dry-run mode

**Files:**
- Modify: `siphon/core/pipeline.py`
- Create: `tests/core/test_pipeline_diff.py`

Wire the Differ into the pipeline. Only runs when `dry_run=True`.

- [ ] **Step 1: Write the failing tests**

Create `tests/core/test_pipeline_diff.py`:

```python
"""Pipeline integration tests for dry-run diff."""

from __future__ import annotations

from pathlib import Path

import pytest
from siphon.config.schema import SiphonConfig
from siphon.core.pipeline import Pipeline
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator


def _config_yaml(tmp_path: Path, on_conflict_yaml: str = "") -> Path:
    """Write a config file with an optional on_conflict block to tmp_path."""
    db_path = tmp_path / "test.db"
    yaml = f"""
name: diff-test
source: {{ type: spreadsheet }}
database: {{ url: "sqlite+aiosqlite:///{db_path}" }}
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: {{ table: companies, column: name }}
    - name: phone
      source: "Phone"
      type: string
      db: {{ table: companies, column: phone }}
  tables:
    companies:
      primary_key: {{ column: id, type: auto_increment }}
{on_conflict_yaml}
pipeline: {{ review: false }}
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _csv(tmp_path: Path, name: str, rows: list[dict]) -> Path:
    """Write a CSV file in tmp_path."""
    p = tmp_path / name
    if not rows:
        p.write_text("Name,Phone\n")
        return p
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row[h]) for h in headers))
    p.write_text("\n".join(lines) + "\n")
    return p


class TestPipelineDryRunDiff:
    async def test_dry_run_without_on_conflict_categorizes_as_insert(self, tmp_path):
        from siphon.config.loader import load_config
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", [
            {"Name": "Acme", "Phone": "111"},
            {"Name": "Beta", "Phone": "222"},
        ])

        config = load_config(config_path)
        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, dry_run=True, no_review=True,
                                    create_tables=False)

        assert result.dry_run is True
        assert result.diff is not None
        assert len(result.diff["insert"]) == 2
        assert result.diff["update"] == []

    async def test_dry_run_with_on_conflict_categorizes_against_db(self, tmp_path):
        from siphon.config.loader import load_config

        on_conflict_yaml = """      on_conflict:
        key: [name]
        action: update
        update_columns: all"""
        config_path = _config_yaml(tmp_path, on_conflict_yaml)

        # First run: insert two rows
        config = load_config(config_path)
        first_csv = _csv(tmp_path, "first.csv", [
            {"Name": "Acme", "Phone": "OLD"},
            {"Name": "Beta", "Phone": "111"},
        ])
        await Pipeline(config).run(first_csv, no_review=True, create_tables=True)

        # Second run as dry-run: should diff against the DB
        config2 = load_config(config_path)
        second_csv = _csv(tmp_path, "second.csv", [
            {"Name": "Acme", "Phone": "NEW"},      # update
            {"Name": "Beta", "Phone": "111"},      # no_change
            {"Name": "Gamma", "Phone": "333"},     # insert
        ])
        result = await Pipeline(config2).run(
            second_csv, dry_run=True, no_review=True
        )

        assert result.dry_run is True
        assert result.diff is not None
        assert len(result.diff["insert"]) == 1
        assert result.diff["insert"][0]["name"] == "Gamma"
        assert len(result.diff["update"]) == 1
        assert result.diff["update"][0]["record"]["name"] == "Acme"
        assert result.diff["update"][0]["changes"]["phone"]["old"] == "OLD"
        assert result.diff["update"][0]["changes"]["phone"]["new"] == "NEW"
        assert len(result.diff["no_change"]) == 1
        assert result.diff["no_change"][0]["name"] == "Beta"

    async def test_non_dry_run_does_not_compute_diff(self, tmp_path):
        from siphon.config.loader import load_config
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", [{"Name": "Acme", "Phone": "111"}])

        config = load_config(config_path)
        result = await Pipeline(config).run(
            csv_path, dry_run=False, no_review=True, create_tables=True
        )

        assert result.dry_run is False
        assert result.diff is None
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/core/test_pipeline_diff.py -v
```

Expected: FAIL — pipeline doesn't compute diff yet.

- [ ] **Step 3: Modify the pipeline**

In `siphon/core/pipeline.py`, find the dry-run block. Currently it returns `result` after deduplication. Update it to compute the diff first.

Add an import at the top:

```python
from siphon.db.differ import Differ
```

Replace the `if dry_run:` block. Find:

```python
        if dry_run:
            logger.info("Dry run complete — no database operations performed")
            return result
```

Replace with:

```python
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
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/core/test_pipeline_diff.py -v
```

Expected: 3 passed.

- [ ] **Step 5: Run full suite for regressions**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/core/pipeline.py tests/core/test_pipeline_diff.py
git commit -m "feat: compute diff in dry-run mode"
```

---

### Task 8: CLI renders diff as a Rich table

**Files:**
- Modify: `siphon/cli.py`
- Create: `tests/test_cli_diff.py`

When the pipeline result has a `diff`, the CLI's `_print_summary` should render it as a Rich table after the existing summary table.

- [ ] **Step 1: Write the failing test**

Create `tests/test_cli_diff.py`:

```python
"""Tests for the CLI's diff rendering."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from siphon.cli import app
from siphon.core.pipeline import PipelineResult

runner = CliRunner()


def _write_valid_config(tmp_path):
    yaml = """
name: test
source: { type: spreadsheet }
database: { url: "sqlite:///t.db" }
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: { table: companies, column: name }
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
pipeline: { review: false }
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


class TestCLIDiffRendering:
    def test_dry_run_renders_diff_table(self, tmp_path):
        config = _write_valid_config(tmp_path)
        result = PipelineResult(
            total_extracted=3,
            total_valid=3,
            dry_run=True,
            diff={
                "insert": [{"name": "Brand-New"}],
                "update": [{
                    "key": {"name": "Acme"},
                    "changes": {"phone": {"old": "OLD", "new": "NEW"}},
                    "record": {"name": "Acme", "phone": "NEW"},
                }],
                "skip": [],
                "no_change": [{"name": "Same"}],
            },
        )

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv", "--config", str(config), "--dry-run", "--no-review",
            ])

        assert cli_result.exit_code == 0
        # Diff table should mention each category
        assert "Insert" in cli_result.output
        assert "Update" in cli_result.output
        assert "No Change" in cli_result.output
        # And the counts
        assert "1" in cli_result.output  # at least one count
        # And at least one update detail
        assert "Acme" in cli_result.output

    def test_non_dry_run_does_not_render_diff(self, tmp_path):
        config = _write_valid_config(tmp_path)
        result = PipelineResult(total_extracted=2, total_inserted=2, dry_run=False)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv", "--config", str(config), "--no-review",
            ])

        assert cli_result.exit_code == 0
        # The diff-specific category labels should NOT appear
        assert "No Change" not in cli_result.output
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_cli_diff.py -v
```

Expected: FAIL — diff isn't rendered yet.

- [ ] **Step 3: Modify `siphon/cli.py`**

Find the `_print_summary` function. Add a call to a new `_print_diff` function at the end of `_print_summary`:

```python
def _print_summary(result: PipelineResult) -> None:
    """Print a Rich table summarising the pipeline result."""
    table = Table(title="Pipeline Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Extracted", str(result.total_extracted))
    table.add_row("Valid", str(result.total_valid))
    table.add_row("Invalid", str(result.total_invalid))
    table.add_row("Duplicates", str(result.total_duplicates))

    if not result.dry_run:
        table.add_row("Inserted", str(result.total_inserted))
    else:
        table.add_row("Inserted", "skipped (dry run)")

    if result.skipped_chunks:
        table.add_row("Skipped Chunks", str(len(result.skipped_chunks)))

    console.print(table)

    if result.diff is not None:
        _print_diff(result.diff)
```

Add the new function:

```python
def _print_diff(diff: dict) -> None:
    """Print the dry-run diff as a Rich table."""
    table = Table(title="Pipeline Diff (dry run)")
    table.add_column("Action", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Insert", str(len(diff.get("insert", []))))
    table.add_row("Update", str(len(diff.get("update", []))))
    table.add_row("Skip", str(len(diff.get("skip", []))))
    table.add_row("No Change", str(len(diff.get("no_change", []))))

    console.print(table)

    # Show update details (up to 20 rows to keep output sane)
    updates = diff.get("update", [])
    if updates:
        console.print("[bold]Updates:[/bold]")
        for u in updates[:20]:
            key_str = ", ".join(f"{k}={v!r}" for k, v in u["key"].items())
            for col, change in u["changes"].items():
                console.print(
                    f"  {key_str} → {col}: {change['old']!r} → {change['new']!r}"
                )
        if len(updates) > 20:
            console.print(f"  ... and {len(updates) - 20} more")
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/test_cli_diff.py -v
```

Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/cli.py tests/test_cli_diff.py
git commit -m "feat: CLI renders dry-run diff as Rich table"
```

---

### Task 9: CLI `--output json` flag

**Files:**
- Modify: `siphon/cli.py`
- Modify: `tests/test_cli_diff.py`

Add `--output {table,json}` flag (default: table). When `json`, print the result as JSON instead of Rich tables.

- [ ] **Step 1: Write failing tests**

Append to `tests/test_cli_diff.py`:

```python
class TestCLIOutputJSON:
    def test_output_json_emits_diff_as_json(self, tmp_path):
        import json
        config = _write_valid_config(tmp_path)
        result = PipelineResult(
            total_extracted=2,
            total_valid=2,
            dry_run=True,
            diff={
                "insert": [{"name": "New"}],
                "update": [],
                "skip": [],
                "no_change": [{"name": "Same"}],
            },
        )

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv",
                "--config", str(config),
                "--dry-run",
                "--no-review",
                "--output", "json",
            ])

        assert cli_result.exit_code == 0
        # Locate the JSON object in stdout
        # Output may include log lines before the JSON; find the first { or [
        text = cli_result.output
        first_brace = text.find("{")
        assert first_brace != -1, f"No JSON in output: {text!r}"
        parsed = json.loads(text[first_brace:])
        assert parsed["total_extracted"] == 2
        assert parsed["dry_run"] is True
        assert parsed["diff"]["insert"][0]["name"] == "New"
        assert parsed["diff"]["no_change"][0]["name"] == "Same"

    def test_output_table_is_default(self, tmp_path):
        """Without --output, table format is used."""
        config = _write_valid_config(tmp_path)
        result = PipelineResult(total_extracted=1, dry_run=True, diff={
            "insert": [{"name": "New"}], "update": [], "skip": [], "no_change": []
        })

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv",
                "--config", str(config),
                "--dry-run",
                "--no-review",
            ])

        assert cli_result.exit_code == 0
        # Should see table rendering, not JSON
        assert "Pipeline Diff" in cli_result.output
```

- [ ] **Step 2: Verify tests fail**

```bash
.venv/bin/pytest tests/test_cli_diff.py::TestCLIOutputJSON -v
```

Expected: FAIL — `--output` flag doesn't exist.

- [ ] **Step 3: Add the flag and JSON renderer to `siphon/cli.py`**

Add `import json` at the top of `siphon/cli.py` (alongside other imports).

In the `run` command signature, add a new option after the existing flags:

```python
    output: str = typer.Option("table", "--output", help="Output format: table | json"),
```

Replace the existing `_print_summary(result)` call inside `run()` with:

```python
        if output == "json":
            _print_json(result)
        else:
            _print_summary(result)
```

Add a new `_print_json` function alongside `_print_summary`:

```python
def _print_json(result: PipelineResult) -> None:
    """Print the pipeline result as JSON for scripting/CI."""
    # Convert dataclass → dict using a custom default for non-serializable types
    from dataclasses import asdict

    def _json_default(o):
        try:
            return str(o)
        except Exception:
            return repr(o)

    payload = asdict(result)
    print(json.dumps(payload, default=_json_default, indent=2))
```

- [ ] **Step 4: Run tests**

```bash
.venv/bin/pytest tests/test_cli_diff.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/cli.py tests/test_cli_diff.py
git commit -m "feat: --output json flag for CLI diff output"
```

---

### Task 10: End-to-end test with file-based DB

**Files:**
- Create: `tests/test_integration_diff.py`

Verify the full flow against a real (file-based) SQLite DB to catch any cross-component regressions.

- [ ] **Step 1: Write the test**

Create `tests/test_integration_diff.py`:

```python
"""End-to-end test for dry-run diff with a real file-based SQLite DB."""

from __future__ import annotations

from pathlib import Path

import pytest
from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _write_yaml(tmp_path: Path, db_path: Path) -> Path:
    yaml = f"""
name: integration-diff
source: {{ type: spreadsheet }}
database: {{ url: "sqlite+aiosqlite:///{db_path}" }}
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: {{ table: companies, column: name }}
    - name: phone
      source: "Phone"
      type: string
      db: {{ table: companies, column: phone }}
  tables:
    companies:
      primary_key: {{ column: id, type: auto_increment }}
      on_conflict:
        key: [name]
        action: update
        update_columns: all
pipeline: {{ review: false }}
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _write_csv(tmp_path: Path, name: str, rows: list[dict]) -> Path:
    p = tmp_path / name
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row[h]) for h in headers))
    p.write_text("\n".join(lines) + "\n")
    return p


class TestDryRunDiffEndToEnd:
    async def test_full_diff_flow(self, tmp_path):
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)

        # First: insert two rows for real
        first_csv = _write_csv(tmp_path, "first.csv", [
            {"Name": "Acme", "Phone": "OLD"},
            {"Name": "Beta", "Phone": "111"},
        ])
        first = await Pipeline(load_config(config_path)).run(
            first_csv, no_review=True, create_tables=True
        )
        assert first.total_inserted == 2

        # Second: dry-run with mixed cases
        second_csv = _write_csv(tmp_path, "second.csv", [
            {"Name": "Acme", "Phone": "NEW"},
            {"Name": "Beta", "Phone": "111"},
            {"Name": "Gamma", "Phone": "333"},
        ])
        second = await Pipeline(load_config(config_path)).run(
            second_csv, dry_run=True, no_review=True
        )

        assert second.dry_run is True
        assert second.total_inserted == 0  # nothing committed
        assert second.diff is not None

        # Insert: Gamma
        assert len(second.diff["insert"]) == 1
        assert second.diff["insert"][0]["name"] == "Gamma"

        # Update: Acme
        assert len(second.diff["update"]) == 1
        u = second.diff["update"][0]
        assert u["key"] == {"name": "Acme"}
        assert u["changes"]["phone"]["old"] == "OLD"
        assert u["changes"]["phone"]["new"] == "NEW"

        # No change: Beta
        assert len(second.diff["no_change"]) == 1
        assert second.diff["no_change"][0]["name"] == "Beta"

    async def test_dry_run_does_not_modify_db(self, tmp_path):
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)

        # Set up initial state
        first = _write_csv(tmp_path, "first.csv", [{"Name": "Acme", "Phone": "OLD"}])
        await Pipeline(load_config(config_path)).run(
            first, no_review=True, create_tables=True
        )

        # Dry-run that would update Acme
        second = _write_csv(tmp_path, "second.csv", [{"Name": "Acme", "Phone": "NEW"}])
        await Pipeline(load_config(config_path)).run(
            second, dry_run=True, no_review=True
        )

        # Verify the DB still has OLD
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT phone FROM companies"))
                phones = [r[0] for r in result.fetchall()]
        finally:
            await engine.dispose()

        assert phones == ["OLD"]
```

- [ ] **Step 2: Run the tests**

```bash
.venv/bin/pytest tests/test_integration_diff.py -v
```

Expected: 2 passed.

- [ ] **Step 3: Run the full suite**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration_diff.py
git commit -m "test: end-to-end dry-run diff integration test"
```

---

### Task 11: README update + version bump + push

**Files:**
- Modify: `README.md`
- Modify: `siphon/__init__.py`

- [ ] **Step 1: Add a "Dry-Run with Diff" section to `README.md`**

Find the existing "## Upserts" section. Insert the following section IMMEDIATELY AFTER the Upserts section (before the next `##` heading):

```markdown
## Dry-Run with Diff

When you run `siphon run --dry-run`, Siphon shows what *would* change before committing anything to the database:

\`\`\`
Pipeline Diff (dry run)
┌──────────┬───────┐
│ Action   │ Count │
├──────────┼───────┤
│ Insert   │     5 │
│ Update   │     3 │
│ Skip     │     0 │
│ No Change│     1 │
└──────────┴───────┘

Updates:
  name='Acme Corp' → phone: '(555) 123-4567' → '(555) 999-8888'
  name='Beta Inc'  → website_url: 'http://beta.io' → 'http://beta.com'
\`\`\`

**Categories:**
- `Insert` — new rows that would be added
- `Update` — existing rows whose values would change
- `Skip` — existing rows that would be skipped (when `on_conflict.action: skip`)
- `No Change` — rows that already match the database

For scripting, use `--output json` to get machine-readable output:

\`\`\`bash
siphon run data.csv --dry-run --output json
\`\`\`

The diff respects the `on_conflict.key` declared on each table. If no `on_conflict` is configured, every record is categorized as `Insert`.
```

(In the actual README, replace `\`\`\`` escapes with literal triple backticks.)

- [ ] **Step 2: Bump version in `siphon/__init__.py`**

Change:

```python
__version__ = "0.3.0a1"
```

to:

```python
__version__ = "0.3.0a2"
```

- [ ] **Step 3: Run the full suite once more**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All pass.

- [ ] **Step 4: Audit for proprietary references**

```bash
grep -ri "workshield\|work.shield" --include="*.py" --include="*.yaml" --include="*.md" --include="*.toml" .
```

Expected: no matches.

- [ ] **Step 5: Commit and push**

```bash
git add README.md siphon/__init__.py
git commit -m "chore: bump version to 0.3.0a2 (Phase 2 complete)"
git push -u origin v3-phase-2-dry-run-diff
```

---

## Verification

After all tasks complete:

1. All tests pass: `.venv/bin/pytest tests/ -q` shows zero failures.
2. The branch `v3-phase-2-dry-run-diff` is pushed to the remote.
3. CLI smoke test: with a populated DB and a CSV, run `siphon run data.csv --dry-run --config siphon.yaml` and verify a diff table appears.
4. JSON smoke test: same command with `--output json` produces parseable JSON containing a `diff` key.

## Out of Scope

The following are deferred to later phases:

- **Per-table diff breakdown** — current implementation infers a single primary table from the first field. Multi-table configs work for inserts/no_change but `update`/`skip` are only computed for the primary table. Phase 4 (audit trail) will revisit per-table tracking.
- **Diff for collection records** — collections are mapped but not diffed against existing collection rows. They're always categorized as `insert`. Phase 4 may extend this.
- **Old vs new for inserts** — only `update` records have field-level change details. Inserts show the full record dict.

## Self-Review Notes

**Spec coverage check:**
- ✅ Pipeline Diff table with Insert/Update/Skip/No Change/Invalid categories — Tasks 4, 5, 8 (Invalid is `total_invalid` from existing pipeline; not in diff dict — diff is post-validation)
- ✅ Update detail rows (`field: old → new`) — Task 8 `_print_diff`
- ✅ JSON output (`--output json`) — Task 9
- ✅ Reuses lookup-by-key from Phase 1 — Task 4 uses `on_conflict.key`

**Type consistency check:**
- `Differ(config, db_engine, model_generator)` — same signature in all tasks.
- `compute_diff(records) -> dict[str, list]` — same return shape across Tasks 2, 3, 4, 5, 7.
- `PipelineResult.diff: dict | None = None` — Task 6, used by Tasks 7, 8, 9, 10.

**Placeholder scan:** None.

**One deviation from spec:** The spec mentions an `Invalid` row in the Diff table. Invalid records are reported via the existing `total_invalid` count in the main Pipeline Summary table — not duplicated in the diff table. The diff is computed AFTER validation, so by definition only valid records are categorized. This is a deliberate simplification that matches the actual data flow.
