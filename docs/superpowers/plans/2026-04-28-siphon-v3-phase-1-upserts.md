# Siphon v3 Phase 1: Upserts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add per-table `on_conflict` configuration so Siphon can update existing rows instead of just inserting new ones.

**Architecture:** A new `OnConflictConfig` Pydantic model on `TableConfig` declares the conflict key and action (`update`, `skip`, `error`). The `Inserter` checks for existing rows by the conflict key before inserting; on match, it dispatches to dialect-specific upsert SQL via SQLAlchemy. The default behavior (`error`) preserves v2 semantics so existing configs keep working.

**Tech Stack:** Python 3.11+, SQLAlchemy 2.0 async, Pydantic 2.0, pytest, aiosqlite (test), asyncpg + aiomysql (production drivers — not required to develop).

**Spec:** `docs/superpowers/specs/2026-04-22-siphon-v3-roadmap.md` (Phase 1 section)

**Branch:** Work on a new branch `v3-phase-1-upserts` cut from `v2-no-llm`.

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/config/schema.py` | `OnConflictConfig` model + `on_conflict` field on `TableConfig` | Modify |
| `siphon/db/upsert.py` | Dialect-aware upsert statement builder | Create |
| `siphon/db/inserter.py` | Use upsert builder when `on_conflict` is configured | Modify |
| `tests/config/test_schema.py` | Tests for `OnConflictConfig` | Modify |
| `tests/db/test_upsert.py` | Tests for upsert statement builder | Create |
| `tests/db/test_inserter_upsert.py` | Integration tests for upsert flow | Create |
| `siphon/cli.py` | Update `INIT_TEMPLATE` with `on_conflict` example | Modify |

The new `siphon/db/upsert.py` keeps dialect-specific SQL out of `inserter.py`. The inserter calls a single function (`build_upsert_statement`) and doesn't care which dialect it's running against.

---

## Task Breakdown

### Task 1: Branch + scaffold

**Files:**
- Create branch only

- [ ] **Step 1: Cut a new branch from v2-no-llm**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v2-no-llm
git pull
git checkout -b v3-phase-1-upserts
```

- [ ] **Step 2: Verify the v2 baseline tests pass**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `863 passed` (no failures).

- [ ] **Step 3: No commit yet** — branch creation alone doesn't need a commit.

---

### Task 2: OnConflictConfig schema model

**Files:**
- Modify: `siphon/config/schema.py`
- Modify: `tests/config/test_schema.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/config/test_schema.py`:

```python
class TestOnConflictConfig:
    def test_default_action_is_error(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"])
        assert cfg.action == "error"
        assert cfg.update_columns == "all"

    def test_action_update(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"], action="update")
        assert cfg.action == "update"

    def test_action_skip(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"], action="skip")
        assert cfg.action == "skip"

    def test_invalid_action_rejected(self):
        from pydantic import ValidationError
        from siphon.config.schema import OnConflictConfig
        with pytest.raises(ValidationError):
            OnConflictConfig(key=["name"], action="merge")

    def test_composite_key(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name", "country_code"])
        assert cfg.key == ["name", "country_code"]

    def test_empty_key_rejected(self):
        from pydantic import ValidationError
        from siphon.config.schema import OnConflictConfig
        with pytest.raises(ValidationError):
            OnConflictConfig(key=[])

    def test_update_columns_specific_list(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"], action="update",
                               update_columns=["phone", "website"])
        assert cfg.update_columns == ["phone", "website"]

    def test_table_config_with_on_conflict(self):
        from siphon.config.schema import TableConfig, PrimaryKeyConfig, OnConflictConfig
        tc = TableConfig(
            primary_key=PrimaryKeyConfig(column="id", type="auto_increment"),
            on_conflict=OnConflictConfig(key=["name"], action="update"),
        )
        assert tc.on_conflict.action == "update"

    def test_table_config_without_on_conflict(self):
        from siphon.config.schema import TableConfig, PrimaryKeyConfig
        tc = TableConfig(
            primary_key=PrimaryKeyConfig(column="id", type="auto_increment"),
        )
        assert tc.on_conflict is None
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
.venv/bin/pytest tests/config/test_schema.py::TestOnConflictConfig -v
```

Expected: ImportError or AttributeError on `OnConflictConfig` — it doesn't exist yet.

- [ ] **Step 3: Add the model and update TableConfig**

In `siphon/config/schema.py`, add after the `PrimaryKeyConfig` class (around line 134) and before `TableConfig`:

```python
class OnConflictConfig(BaseModel):
    """Conflict resolution strategy for inserts that hit an existing row."""

    model_config = ConfigDict(populate_by_name=True)

    key: list[str] = Field(min_length=1)  # Field names that form the unique conflict key
    action: Literal["update", "skip", "error"] = "error"
    update_columns: Literal["all"] | list[str] = "all"
```

Then modify `TableConfig` to add the optional `on_conflict` field:

```python
class TableConfig(BaseModel):
    """Configuration for a single database table."""

    model_config = ConfigDict(populate_by_name=True)

    primary_key: PrimaryKeyConfig
    on_conflict: OnConflictConfig | None = None
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
.venv/bin/pytest tests/config/test_schema.py::TestOnConflictConfig -v
```

Expected: 9 passed.

- [ ] **Step 5: Run the full schema test suite**

```bash
.venv/bin/pytest tests/config/test_schema.py -q
```

Expected: All schema tests pass (no regressions).

- [ ] **Step 6: Commit**

```bash
git add siphon/config/schema.py tests/config/test_schema.py
git commit -m "feat: OnConflictConfig schema model with key/action/update_columns"
```

---

### Task 3: Cross-validate on_conflict.key field names

**Files:**
- Modify: `siphon/config/loader.py`
- Modify: `tests/config/test_loader.py`

`on_conflict.key` references field names. We need to validate those exist in `schema.fields` (or in a collection field) at config-load time.

- [ ] **Step 1: Write the failing test**

Add to `tests/config/test_loader.py`:

```python
def test_on_conflict_key_unknown_field_raises(tmp_path):
    config_yaml = """
name: test-pipeline
source: { type: spreadsheet }
database: { url: "sqlite:///t.db" }
schema:
  fields:
    - name: company_name
      source: "Name"
      type: string
      db: { table: companies, column: name }
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [nonexistent_field]
        action: update
pipeline: { review: false }
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(config_yaml)
    from siphon.utils.errors import ConfigError
    from siphon.config.loader import load_config
    with pytest.raises(ConfigError, match="on_conflict.*nonexistent_field"):
        load_config(p)


def test_on_conflict_key_valid_field_loads(tmp_path):
    config_yaml = """
name: test-pipeline
source: { type: spreadsheet }
database: { url: "sqlite:///t.db" }
schema:
  fields:
    - name: company_name
      source: "Name"
      type: string
      db: { table: companies, column: name }
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [company_name]
        action: update
pipeline: { review: false }
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(config_yaml)
    from siphon.config.loader import load_config
    cfg = load_config(p)
    assert cfg.schema_.tables["companies"].on_conflict.action == "update"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/config/test_loader.py::test_on_conflict_key_unknown_field_raises tests/config/test_loader.py::test_on_conflict_key_valid_field_loads -v
```

Expected: `test_on_conflict_key_unknown_field_raises` fails (no error raised).

- [ ] **Step 3: Add validation to `siphon/config/loader.py`**

Find the `_cross_validate` function and add this block before the function returns:

```python
def _cross_validate(config: SiphonConfig) -> None:
    # ... existing validation code stays unchanged above ...

    # Validate on_conflict.key references known field names
    known_field_names = {f.name for f in config.schema_.fields}
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
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/config/test_loader.py -q
```

Expected: All loader tests pass, including the 2 new ones.

- [ ] **Step 5: Commit**

```bash
git add siphon/config/loader.py tests/config/test_loader.py
git commit -m "feat: cross-validate on_conflict.key against known field names"
```

---

### Task 4: Upsert statement builder — skeleton + dialect detection

**Files:**
- Create: `siphon/db/upsert.py`
- Create: `tests/db/test_upsert.py`

This task creates the public API of the upsert builder. The actual SQL generation for each dialect comes in subsequent tasks.

- [ ] **Step 1: Write the failing test**

Create `tests/db/test_upsert.py`:

```python
"""Tests for the dialect-aware upsert statement builder."""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table


def _make_table():
    """Build a simple SQLAlchemy Table for testing."""
    md = MetaData()
    return Table(
        "companies",
        md,
        Column("id", Integer, primary_key=True),
        Column("name", String(255), unique=True),
        Column("phone", String(20)),
        Column("website", String(500)),
    )


class TestDetectDialect:
    def test_sqlite_dialect(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("sqlite+aiosqlite:///x.db") == "sqlite"

    def test_postgres_dialect(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("postgresql+asyncpg://u:p@h/d") == "postgresql"

    def test_mysql_dialect(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("mysql+aiomysql://u:p@h/d") == "mysql"

    def test_unknown_dialect_returns_generic(self):
        from siphon.db.upsert import detect_dialect
        assert detect_dialect("oracle://u:p@h/d") == "generic"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/db/test_upsert.py -v
```

Expected: ImportError on `siphon.db.upsert`.

- [ ] **Step 3: Create the module skeleton**

Create `siphon/db/upsert.py`:

```python
"""Dialect-aware upsert statement builder.

Builds INSERT ... ON CONFLICT statements for each supported SQL dialect.
The Inserter calls build_upsert_statement() and doesn't care which dialect
is in use — that detection happens here.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import Table

logger = logging.getLogger("siphon")


def detect_dialect(database_url: str) -> str:
    """Map a SQLAlchemy URL to a dialect name.

    Returns one of: "sqlite", "postgresql", "mysql", "generic".
    """
    url_lower = database_url.lower()
    if url_lower.startswith("sqlite"):
        return "sqlite"
    if url_lower.startswith("postgresql") or url_lower.startswith("postgres"):
        return "postgresql"
    if url_lower.startswith("mysql") or url_lower.startswith("mariadb"):
        return "mysql"
    return "generic"
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_upsert.py -v
```

Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/upsert.py tests/db/test_upsert.py
git commit -m "feat: upsert module skeleton with dialect detection"
```

---

### Task 5: SQLite upsert statement

**Files:**
- Modify: `siphon/db/upsert.py`
- Modify: `tests/db/test_upsert.py`

SQLite uses `INSERT ... ON CONFLICT(cols) DO UPDATE SET ...` (since SQLite 3.24).

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_upsert.py`:

```python
class TestBuildUpsertSQLite:
    def test_action_update_all_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "INSERT INTO companies" in compiled
        assert "ON CONFLICT (name) DO UPDATE" in compiled
        assert "phone" in compiled
        assert "website" in compiled

    def test_action_update_specific_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns=["phone"],
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "DO UPDATE SET phone" in compiled
        assert "website" not in compiled.split("DO UPDATE")[1]

    def test_action_skip_uses_do_nothing(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT (name) DO NOTHING" in compiled

    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT" not in compiled

    def test_composite_conflict_key(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="sqlite",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name", "phone"],
            action="update",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "ON CONFLICT (name, phone)" in compiled
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/db/test_upsert.py::TestBuildUpsertSQLite -v
```

Expected: ImportError on `build_upsert_statement`.

- [ ] **Step 3: Implement `build_upsert_statement` and SQLite branch**

Add to `siphon/db/upsert.py`:

```python
from sqlalchemy.dialects.sqlite import insert as sqlite_insert


def build_upsert_statement(
    *,
    dialect: str,
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    """Build an upsert (INSERT ... ON CONFLICT) statement for the given dialect.

    Args:
        dialect: One of "sqlite", "postgresql", "mysql", "generic"
        table: SQLAlchemy Table object
        row: Dict of column name → value to insert
        conflict_key: List of column names that form the unique conflict target
        action: "update" | "skip" | "error"
        update_columns: "all" or list of column names to update on conflict

    Returns:
        A SQLAlchemy executable Insert statement.
    """
    if dialect == "sqlite":
        return _build_sqlite_upsert(table, row, conflict_key, action, update_columns)
    raise NotImplementedError(f"Upsert not yet supported for dialect: {dialect}")


def _build_sqlite_upsert(
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    stmt = sqlite_insert(table).values(**row)

    if action == "error":
        return stmt

    if action == "skip":
        return stmt.on_conflict_do_nothing(index_elements=conflict_key)

    # action == "update"
    if update_columns == "all":
        # Update all columns except the conflict key columns
        update_set = {
            col.name: stmt.excluded[col.name]
            for col in table.columns
            if col.name not in conflict_key and col.name in row
        }
    else:
        update_set = {
            col_name: stmt.excluded[col_name]
            for col_name in update_columns
            if col_name in row
        }

    return stmt.on_conflict_do_update(
        index_elements=conflict_key,
        set_=update_set,
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_upsert.py -v
```

Expected: 9 passed (4 dialect detection + 5 SQLite).

- [ ] **Step 5: Commit**

```bash
git add siphon/db/upsert.py tests/db/test_upsert.py
git commit -m "feat: SQLite upsert statement builder"
```

---

### Task 6: PostgreSQL upsert statement

**Files:**
- Modify: `siphon/db/upsert.py`
- Modify: `tests/db/test_upsert.py`

PostgreSQL uses the same `ON CONFLICT` syntax as SQLite (which copied it from PostgreSQL). The SQLAlchemy API is structurally identical via `sqlalchemy.dialects.postgresql.insert`.

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_upsert.py`:

```python
class TestBuildUpsertPostgres:
    def test_action_update_all_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="postgresql",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        from sqlalchemy.dialects import postgresql
        compiled = str(stmt.compile(dialect=postgresql.dialect()))
        assert "INSERT INTO companies" in compiled
        assert "ON CONFLICT (name) DO UPDATE" in compiled

    def test_action_skip(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="postgresql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        from sqlalchemy.dialects import postgresql
        compiled = str(stmt.compile(dialect=postgresql.dialect()))
        assert "ON CONFLICT (name) DO NOTHING" in compiled

    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="postgresql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        from sqlalchemy.dialects import postgresql
        compiled = str(stmt.compile(dialect=postgresql.dialect()))
        assert "ON CONFLICT" not in compiled
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/db/test_upsert.py::TestBuildUpsertPostgres -v
```

Expected: NotImplementedError for postgresql dialect.

- [ ] **Step 3: Implement PostgreSQL branch**

In `siphon/db/upsert.py`, add this import:

```python
from sqlalchemy.dialects.postgresql import insert as postgres_insert
```

And add a `_build_postgres_upsert` function (structurally identical to SQLite, just using `postgres_insert`):

```python
def _build_postgres_upsert(
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    stmt = postgres_insert(table).values(**row)

    if action == "error":
        return stmt

    if action == "skip":
        return stmt.on_conflict_do_nothing(index_elements=conflict_key)

    if update_columns == "all":
        update_set = {
            col.name: stmt.excluded[col.name]
            for col in table.columns
            if col.name not in conflict_key and col.name in row
        }
    else:
        update_set = {
            col_name: stmt.excluded[col_name]
            for col_name in update_columns
            if col_name in row
        }

    return stmt.on_conflict_do_update(
        index_elements=conflict_key,
        set_=update_set,
    )
```

Then update the dispatch in `build_upsert_statement`:

```python
def build_upsert_statement(
    *,
    dialect: str,
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    if dialect == "sqlite":
        return _build_sqlite_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "postgresql":
        return _build_postgres_upsert(table, row, conflict_key, action, update_columns)
    raise NotImplementedError(f"Upsert not yet supported for dialect: {dialect}")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_upsert.py -v
```

Expected: 12 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/upsert.py tests/db/test_upsert.py
git commit -m "feat: PostgreSQL upsert statement builder"
```

---

### Task 7: MySQL upsert statement

**Files:**
- Modify: `siphon/db/upsert.py`
- Modify: `tests/db/test_upsert.py`

MySQL uses `INSERT ... ON DUPLICATE KEY UPDATE`. Note: MySQL doesn't take an explicit conflict-target column list — it uses any unique index. We document this difference but the API stays the same.

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_upsert.py`:

```python
class TestBuildUpsertMySQL:
    def test_action_update_all_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "INSERT INTO companies" in compiled
        assert "ON DUPLICATE KEY UPDATE" in compiled

    def test_action_update_specific_columns(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme", "phone": "555", "website": "acme.com"},
            conflict_key=["name"],
            action="update",
            update_columns=["phone"],
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY UPDATE" in compiled
        # 'phone' should appear in the UPDATE clause; 'website' should not
        update_part = compiled.split("ON DUPLICATE KEY UPDATE")[1]
        assert "phone" in update_part
        assert "website" not in update_part

    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        assert "ON DUPLICATE KEY" not in compiled

    def test_action_skip_uses_no_op_update(self):
        """MySQL has no DO NOTHING; emulate by setting a column to itself."""
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="mysql",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        from sqlalchemy.dialects import mysql
        compiled = str(stmt.compile(dialect=mysql.dialect()))
        # No-op skip: set the conflict key to itself
        assert "ON DUPLICATE KEY UPDATE" in compiled
        assert "name = name" in compiled or "name=name" in compiled
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/db/test_upsert.py::TestBuildUpsertMySQL -v
```

Expected: NotImplementedError for mysql dialect.

- [ ] **Step 3: Implement MySQL branch**

In `siphon/db/upsert.py`, add this import:

```python
from sqlalchemy.dialects.mysql import insert as mysql_insert
```

Add `_build_mysql_upsert`:

```python
def _build_mysql_upsert(
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    stmt = mysql_insert(table).values(**row)

    if action == "error":
        return stmt

    if action == "skip":
        # MySQL has no DO NOTHING; no-op by setting the first conflict key column to itself.
        first_key = conflict_key[0]
        return stmt.on_duplicate_key_update(**{first_key: getattr(stmt.inserted, first_key)})

    # action == "update"
    if update_columns == "all":
        update_set = {
            col.name: getattr(stmt.inserted, col.name)
            for col in table.columns
            if col.name not in conflict_key and col.name in row
        }
    else:
        update_set = {
            col_name: getattr(stmt.inserted, col_name)
            for col_name in update_columns
            if col_name in row
        }

    return stmt.on_duplicate_key_update(**update_set)
```

Update the dispatch:

```python
def build_upsert_statement(
    *,
    dialect: str,
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    if dialect == "sqlite":
        return _build_sqlite_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "postgresql":
        return _build_postgres_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "mysql":
        return _build_mysql_upsert(table, row, conflict_key, action, update_columns)
    raise NotImplementedError(f"Upsert not yet supported for dialect: {dialect}")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_upsert.py -v
```

Expected: 16 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/upsert.py tests/db/test_upsert.py
git commit -m "feat: MySQL upsert statement builder"
```

---

### Task 8: Generic fallback (select-then-update)

**Files:**
- Modify: `siphon/db/upsert.py`
- Modify: `tests/db/test_upsert.py`

For unsupported dialects, fall back to a non-atomic select-then-update path. Document the race-condition warning per the spec.

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_upsert.py`:

```python
class TestBuildUpsertGeneric:
    def test_action_error_returns_plain_insert(self):
        from siphon.db.upsert import build_upsert_statement
        table = _make_table()
        stmt = build_upsert_statement(
            dialect="generic",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="error",
            update_columns="all",
        )
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))
        assert "INSERT INTO companies" in compiled
        assert "ON CONFLICT" not in compiled
        assert "ON DUPLICATE KEY" not in compiled

    def test_action_update_returns_marker_for_caller(self):
        """Generic fallback returns a special object signalling 'use select-then-update'."""
        from siphon.db.upsert import build_upsert_statement, GenericUpsertPlan
        table = _make_table()
        plan = build_upsert_statement(
            dialect="generic",
            table=table,
            row={"name": "Acme", "phone": "555"},
            conflict_key=["name"],
            action="update",
            update_columns="all",
        )
        assert isinstance(plan, GenericUpsertPlan)
        assert plan.action == "update"
        assert plan.conflict_key == ["name"]
        assert plan.row == {"name": "Acme", "phone": "555"}
        assert plan.update_columns == "all"
        assert plan.table is table

    def test_action_skip_returns_marker(self):
        from siphon.db.upsert import build_upsert_statement, GenericUpsertPlan
        table = _make_table()
        plan = build_upsert_statement(
            dialect="generic",
            table=table,
            row={"name": "Acme"},
            conflict_key=["name"],
            action="skip",
            update_columns="all",
        )
        assert isinstance(plan, GenericUpsertPlan)
        assert plan.action == "skip"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/db/test_upsert.py::TestBuildUpsertGeneric -v
```

Expected: ImportError on `GenericUpsertPlan` and NotImplementedError for generic dialect.

- [ ] **Step 3: Implement generic fallback**

In `siphon/db/upsert.py`, add at the top after imports:

```python
from dataclasses import dataclass
from sqlalchemy import insert as sa_insert


@dataclass
class GenericUpsertPlan:
    """Plan for a select-then-update upsert.

    Used for dialects that have no native upsert. The Inserter executes
    this in two steps: SELECT to check existence, then INSERT or UPDATE.

    NOT atomic — there is a race condition window between the SELECT and
    the subsequent INSERT/UPDATE. Concurrent writers may both see "no row"
    and both INSERT, causing a unique constraint violation.
    """
    table: Table
    row: dict[str, Any]
    conflict_key: list[str]
    action: str
    update_columns: str | list[str]
```

Add `_build_generic_upsert`:

```python
def _build_generic_upsert(
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    if action == "error":
        return sa_insert(table).values(**row)

    logger.warning(
        "Using non-atomic select-then-update upsert fallback. "
        "Concurrent writers may cause unique constraint violations."
    )
    return GenericUpsertPlan(
        table=table,
        row=row,
        conflict_key=conflict_key,
        action=action,
        update_columns=update_columns,
    )
```

Update the dispatch:

```python
def build_upsert_statement(
    *,
    dialect: str,
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    if dialect == "sqlite":
        return _build_sqlite_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "postgresql":
        return _build_postgres_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "mysql":
        return _build_mysql_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "generic":
        return _build_generic_upsert(table, row, conflict_key, action, update_columns)
    raise NotImplementedError(f"Unknown dialect: {dialect}")
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_upsert.py -v
```

Expected: 19 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/db/upsert.py tests/db/test_upsert.py
git commit -m "feat: generic select-then-update upsert fallback"
```

---

### Task 9: Wire upsert into Inserter — base case (action: error)

**Files:**
- Modify: `siphon/db/inserter.py`
- Create: `tests/db/test_inserter_upsert.py`

The first inserter integration step: ensure `action: error` (default) preserves v2 behavior. No code change needed in the inserter for this case — but we add an integration test to lock in the behavior.

- [ ] **Step 1: Write the failing test**

Create `tests/db/test_inserter_upsert.py`:

```python
"""Integration tests for upsert behavior in the Inserter."""

from __future__ import annotations

import pytest
from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator
from siphon.utils.errors import DatabaseError


def _make_config(on_conflict: dict | None = None) -> SiphonConfig:
    """Build a minimal config with one table, optionally with on_conflict."""
    table_cfg = {
        "primary_key": {"column": "id", "type": "auto_increment"},
    }
    if on_conflict is not None:
        table_cfg["on_conflict"] = on_conflict

    return SiphonConfig.model_validate({
        "name": "upsert-test",
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
async def db_setup():
    """Create an in-memory SQLite DB with a unique constraint on name."""
    config = _make_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()

    # Add a unique constraint on the 'name' column for conflict testing
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_companies_name"))

    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionError:
    async def test_default_action_raises_on_conflict(self, db_setup):
        """Without on_conflict, a duplicate insert raises DatabaseError."""
        config, engine, model_gen = db_setup
        inserter = Inserter(config, engine, model_gen)

        # First insert succeeds
        await inserter.insert([{"name": "Acme", "phone": "111"}])

        # Second insert with same name conflicts → DatabaseError
        with pytest.raises(DatabaseError):
            await inserter.insert([{"name": "Acme", "phone": "222"}])
```

- [ ] **Step 2: Run the test**

```bash
.venv/bin/pytest tests/db/test_inserter_upsert.py::TestActionError -v
```

Expected: PASS — this test should pass without any code change because v2 already raises on conflict.

- [ ] **Step 3: No production code change needed for this task — just lock in baseline**

- [ ] **Step 4: Commit**

```bash
git add tests/db/test_inserter_upsert.py
git commit -m "test: lock in baseline error-on-conflict behavior"
```

---

### Task 10: Wire upsert into Inserter — action: skip

**Files:**
- Modify: `siphon/db/inserter.py`
- Modify: `tests/db/test_inserter_upsert.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_inserter_upsert.py`:

```python
@pytest.fixture
async def db_setup_with_skip():
    """DB setup with on_conflict.action=skip configured."""
    config = _make_config({
        "key": ["name"],
        "action": "skip",
    })
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_companies_name"))
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionSkip:
    async def test_skip_does_not_raise(self, db_setup_with_skip):
        """With action=skip, duplicate inserts are silently ignored."""
        config, engine, model_gen = db_setup_with_skip
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "111"}])
        # No exception
        await inserter.insert([{"name": "Acme", "phone": "222"}])

    async def test_skip_preserves_original_row(self, db_setup_with_skip):
        """The original row's values are unchanged after skip."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_with_skip
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "ORIGINAL"}])
        await inserter.insert([{"name": "Acme", "phone": "NEW"}])

        async with engine.session() as session:
            result = await session.execute(
                select(model_gen.models["companies"].phone)
            )
            phones = [r[0] for r in result]

        assert phones == ["ORIGINAL"]
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
.venv/bin/pytest tests/db/test_inserter_upsert.py::TestActionSkip -v
```

Expected: Both tests fail because v2 inserter still uses plain INSERT.

- [ ] **Step 3: Modify the Inserter to use upsert statements**

In `siphon/db/inserter.py`, find the inner insert loop. Currently it uses `session.add(instance); await session.flush()`. We need to replace this with `build_upsert_statement` when `on_conflict` is configured.

First, add imports at the top:

```python
from siphon.db.upsert import (
    GenericUpsertPlan,
    build_upsert_statement,
    detect_dialect,
)
```

Then in `Inserter.__init__`, store the dialect:

```python
def __init__(self, config, db_engine, model_generator):
    self._config = config
    self._db = db_engine
    self._models = model_generator.models
    self._generator = model_generator
    self._lookup_cache = defaultdict(dict)
    self._dialect = detect_dialect(config.database.url)
```

Then in the insert loop (the inner `for table_name in table_order` block), replace the `session.add(instance); await session.flush()` block with logic that picks between native ORM insert (when no `on_conflict`) and upsert statement (when configured).

Find this section in `insert()`:

```python
# Insert
instance = model(**row_data)
session.add(instance)
await session.flush()

# Capture the PK
pk_value = getattr(instance, pk_config.column)
record_ids[table_name] = pk_value
```

Replace it with:

```python
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
```

Add a new method on the Inserter class:

```python
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
    # Translate field names in conflict_key to DB column names
    db_conflict_key = self._field_names_to_columns(on_conflict_cfg.key)

    # Translate update_columns from field names to DB column names if specific list
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

    # Look up the PK by querying for the row using the conflict key
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

    # SELECT to check existence
    pk_col = getattr(model, pk_config.column)
    select_stmt = select(pk_col)
    for col_name in plan.conflict_key:
        select_stmt = select_stmt.where(
            getattr(model, col_name) == plan.row[col_name]
        )
    existing = (await session.execute(select_stmt)).scalar_one_or_none()

    if existing is None:
        # No existing row — INSERT
        await session.execute(sa_insert(plan.table).values(**plan.row))
        return (await session.execute(select_stmt)).scalar_one()

    if plan.action == "skip":
        return existing

    # action == "update"
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
```

- [ ] **Step 4: Run the skip tests**

```bash
.venv/bin/pytest tests/db/test_inserter_upsert.py::TestActionSkip -v
```

Expected: 2 passed.

- [ ] **Step 5: Run the full inserter test suite to check for regressions**

```bash
.venv/bin/pytest tests/db/ -q
```

Expected: All db tests pass.

- [ ] **Step 6: Commit**

```bash
git add siphon/db/inserter.py tests/db/test_inserter_upsert.py
git commit -m "feat: wire skip-on-conflict into Inserter via upsert builder"
```

---

### Task 11: Wire upsert into Inserter — action: update (all columns)

**Files:**
- Modify: `tests/db/test_inserter_upsert.py`

The `_execute_upsert` method already handles `action: update` from Task 10. This task adds tests to verify the update path works end-to-end.

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_inserter_upsert.py`:

```python
@pytest.fixture
async def db_setup_with_update_all():
    """DB setup with on_conflict.action=update, update_columns=all."""
    config = _make_config({
        "key": ["name"],
        "action": "update",
        "update_columns": "all",
    })
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_companies_name"))
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionUpdateAll:
    async def test_update_changes_all_non_key_columns(self, db_setup_with_update_all):
        """On conflict, all non-key columns are updated."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_with_update_all
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "ORIGINAL"}])
        await inserter.insert([{"name": "Acme", "phone": "UPDATED"}])

        async with engine.session() as session:
            result = await session.execute(
                select(model_gen.models["companies"].phone)
            )
            phones = [r[0] for r in result]

        assert phones == ["UPDATED"]

    async def test_update_does_not_create_duplicate_row(self, db_setup_with_update_all):
        """Upsert preserves the original row's PK; no duplicate is inserted."""
        from sqlalchemy import select, func
        config, engine, model_gen = db_setup_with_update_all
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "111"}])
        await inserter.insert([{"name": "Acme", "phone": "222"}])

        async with engine.session() as session:
            result = await session.execute(
                select(func.count()).select_from(model_gen.models["companies"])
            )
            count = result.scalar()

        assert count == 1
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_inserter_upsert.py::TestActionUpdateAll -v
```

Expected: 2 passed (the inserter logic from Task 10 already handles this).

- [ ] **Step 3: Commit**

```bash
git add tests/db/test_inserter_upsert.py
git commit -m "test: verify update-all upsert path"
```

---

### Task 12: Wire upsert into Inserter — action: update (specific columns)

**Files:**
- Modify: `tests/db/test_inserter_upsert.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_inserter_upsert.py`. We need a config that has more than one non-key column so we can verify only some get updated. Add a helper:

```python
def _make_config_with_extra_column(on_conflict: dict | None = None) -> SiphonConfig:
    """Config with an extra 'website' column for selective-update testing."""
    table_cfg = {
        "primary_key": {"column": "id", "type": "auto_increment"},
    }
    if on_conflict is not None:
        table_cfg["on_conflict"] = on_conflict

    return SiphonConfig.model_validate({
        "name": "upsert-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "companies", "column": "phone"}},
                {"name": "website", "source": "Website", "type": "string",
                 "db": {"table": "companies", "column": "website"}},
            ],
            "tables": {"companies": table_cfg},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def db_setup_with_update_specific():
    """DB setup with update_columns=['phone'] only."""
    config = _make_config_with_extra_column({
        "key": ["name"],
        "action": "update",
        "update_columns": ["phone"],
    })
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_companies_name"))
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionUpdateSpecific:
    async def test_only_listed_columns_are_updated(self, db_setup_with_update_specific):
        """update_columns=['phone'] updates phone but leaves website unchanged."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_with_update_specific
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([
            {"name": "Acme", "phone": "111", "website": "ORIGINAL"}
        ])
        await inserter.insert([
            {"name": "Acme", "phone": "222", "website": "NEW"}
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            result = await session.execute(
                select(companies.phone, companies.website)
            )
            row = result.one()

        assert row.phone == "222"  # updated
        assert row.website == "ORIGINAL"  # not in update_columns, unchanged
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_inserter_upsert.py::TestActionUpdateSpecific -v
```

Expected: 1 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/db/test_inserter_upsert.py
git commit -m "test: verify update with specific column list"
```

---

### Task 13: Composite conflict key

**Files:**
- Modify: `tests/db/test_inserter_upsert.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/db/test_inserter_upsert.py`:

```python
def _make_config_with_composite_key(on_conflict: dict) -> SiphonConfig:
    """Config with two-column unique key."""
    return SiphonConfig.model_validate({
        "name": "upsert-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "country_code", "source": "Country", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "country_code"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "companies", "column": "phone"}},
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                    "on_conflict": on_conflict,
                },
            },
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def db_setup_composite_key():
    config = _make_config_with_composite_key({
        "key": ["name", "country_code"],
        "action": "update",
    })
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", "country_code", name="uq_name_country"))
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestCompositeKey:
    async def test_same_name_different_country_inserts_separately(self, db_setup_composite_key):
        """Same name, different country = two separate rows."""
        from sqlalchemy import select, func
        config, engine, model_gen = db_setup_composite_key
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "country_code": "US", "phone": "1"}])
        await inserter.insert([{"name": "Acme", "country_code": "CA", "phone": "2"}])

        async with engine.session() as session:
            result = await session.execute(
                select(func.count()).select_from(model_gen.models["companies"])
            )
            assert result.scalar() == 2

    async def test_same_name_and_country_updates(self, db_setup_composite_key):
        """Same name AND country = update existing row."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_composite_key
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "country_code": "US", "phone": "OLD"}])
        await inserter.insert([{"name": "Acme", "country_code": "US", "phone": "NEW"}])

        async with engine.session() as session:
            result = await session.execute(
                select(model_gen.models["companies"].phone)
            )
            phones = [r[0] for r in result]

        assert phones == ["NEW"]
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
.venv/bin/pytest tests/db/test_inserter_upsert.py::TestCompositeKey -v
```

Expected: 2 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/db/test_inserter_upsert.py
git commit -m "test: verify composite conflict key handling"
```

---

### Task 14: Update INIT_TEMPLATE with on_conflict example

**Files:**
- Modify: `siphon/cli.py`
- Modify: `tests/test_init_template.py`

- [ ] **Step 1: Write the failing test**

Add to `tests/test_init_template.py`:

```python
def test_init_template_includes_on_conflict_example(tmp_path, monkeypatch):
    """The init template should include a commented on_conflict example."""
    from typer.testing import CliRunner
    from siphon.cli import app
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    runner.invoke(app, ["init"], input="y\n")

    content = (tmp_path / "siphon.yaml").read_text()
    assert "on_conflict" in content
    assert "action:" in content  # the action field is mentioned
    # All three actions documented
    assert "update" in content
    assert "skip" in content
    assert "error" in content
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
.venv/bin/pytest tests/test_init_template.py::test_init_template_includes_on_conflict_example -v
```

Expected: FAIL — `on_conflict` not in template.

- [ ] **Step 3: Update the INIT_TEMPLATE in `siphon/cli.py`**

Find the `tables:` block in the `INIT_TEMPLATE` constant. After the existing `companies:` block, add a commented example:

```yaml
  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment  # auto_increment | uuid

      # Conflict resolution (optional) — what to do when a row with the same
      # unique key already exists in the database.
      # on_conflict:
      #   key: [name]              # field names that form the unique key (composite supported)
      #   action: update           # update | skip | error (default: error)
      #   update_columns: all      # all | [list of column names to update]
```

- [ ] **Step 4: Run the test**

```bash
.venv/bin/pytest tests/test_init_template.py::test_init_template_includes_on_conflict_example -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add siphon/cli.py tests/test_init_template.py
git commit -m "feat: document on_conflict in init template"
```

---

### Task 15: Update README with upsert section

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a new section to `README.md`**

After the "Field Types" section and before "Transforms", insert:

```markdown
## Upserts

By default, inserting a row that conflicts with an existing unique key raises an error. To enable insert-or-update behavior, declare an `on_conflict` policy on a table:

\`\`\`yaml
schema:
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [name]              # field names that form the unique conflict key
        action: update           # update | skip | error (default: error)
        update_columns: all      # all | [list of column names]
\`\`\`

**Actions:**
- `update` — update the existing row with new values (true upsert)
- `skip` — silently keep the existing row, ignore the new one
- `error` — fail the transaction (default)

**Composite keys:** `key` accepts multiple field names. All must match for a row to be considered a conflict.

**Selective updates:** `update_columns` defaults to `all` (every non-key column). Provide a list to update only specific columns; others are preserved from the existing row.

**Database support:** Native upserts on PostgreSQL, MySQL, MariaDB, and SQLite (3.24+). For other dialects, Siphon falls back to a non-atomic select-then-update path — concurrent writers may cause unique constraint violations on the fallback path.
```

(Note: in the actual README, replace the `\`\`\`` escapes with backticks. The escape is just so this plan file renders correctly.)

- [ ] **Step 2: No automated test for README content — verify visually**

```bash
head -100 README.md
```

Expected: Upserts section visible after Field Types.

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add upsert section to README"
```

---

### Task 16: End-to-end pipeline test with upserts

**Files:**
- Create: `tests/test_integration_upsert.py`
- Create: `tests/fixtures/companies_v1.csv`
- Create: `tests/fixtures/companies_v2.csv`
- Create: `tests/fixtures/companies_upsert_config.yaml`

Verify the full pipeline (load → map → validate → upsert) works with `on_conflict.action: update`.

- [ ] **Step 1: Create the fixture CSVs**

`tests/fixtures/companies_v1.csv`:

```csv
Name,Phone,Website
Acme Corp,555-1111,acme.com
Beta Inc,555-2222,beta.io
```

`tests/fixtures/companies_v2.csv`:

```csv
Name,Phone,Website
Acme Corp,555-9999,acme-new.com
Gamma LLC,555-3333,gamma.org
```

`tests/fixtures/companies_upsert_config.yaml`:

```yaml
name: companies-upsert-test

source:
  type: spreadsheet

database:
  url: "sqlite+aiosqlite://"  # overridden in test

schema:
  fields:
    - name: company_name
      source: "Name"
      type: string
      required: true
      db: { table: companies, column: name }
    - name: phone
      source: "Phone"
      type: phone
      db: { table: companies, column: phone_number }
    - name: website
      source: "Website"
      type: url
      db: { table: companies, column: website_url }

  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [company_name]
        action: update
        update_columns: all

pipeline:
  review: false
  log_level: info
```

- [ ] **Step 2: Write the failing test**

Create `tests/test_integration_upsert.py`:

```python
"""End-to-end test for the upsert flow."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_config(tmp_path: Path):
    config = load_config(FIXTURES_DIR / "companies_upsert_config.yaml")
    db_path = tmp_path / "test.db"
    config.database.url = f"sqlite+aiosqlite:///{db_path}"
    return config


async def _run(config, csv_name: str):
    pipeline = Pipeline(config)
    return await pipeline.run(
        FIXTURES_DIR / csv_name,
        create_tables=True,
        no_review=True,
    )


async def _query(db_url: str, sql: str):
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            return result.fetchall()
    finally:
        await engine.dispose()


class TestUpsertEndToEnd:
    async def test_first_run_inserts_all(self, tmp_path):
        """First run with v1 CSV inserts 2 rows."""
        config = _load_config(tmp_path)
        result = await _run(config, "companies_v1.csv")
        assert result.total_inserted == 2

        rows = await _query(config.database.url, "SELECT name, phone_number FROM companies ORDER BY name")
        assert len(rows) == 2
        assert rows[0][0] == "Acme Corp"
        assert rows[0][1] == "(555) 555-1111"

    async def test_second_run_updates_existing_and_inserts_new(self, tmp_path):
        """Second run with v2 CSV updates Acme, leaves Beta alone, adds Gamma."""
        config = _load_config(tmp_path)

        # First run: insert v1
        await _run(config, "companies_v1.csv")

        # Second run: v2 has updated Acme, no Beta, new Gamma
        await _run(config, "companies_v2.csv")

        rows = await _query(
            config.database.url,
            "SELECT name, phone_number FROM companies ORDER BY name"
        )

        # Expect: Acme updated, Beta untouched, Gamma added → 3 rows
        assert len(rows) == 3

        names = {r[0]: r[1] for r in rows}
        assert names["Acme Corp"] == "(555) 555-9999"  # updated
        assert names["Beta Inc"] == "(555) 555-2222"   # unchanged
        assert names["Gamma LLC"] == "(555) 555-3333"  # new
```

- [ ] **Step 3: Run the tests**

```bash
.venv/bin/pytest tests/test_integration_upsert.py -v
```

Expected: 2 passed.

- [ ] **Step 4: Run the full test suite to ensure no regressions**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All tests pass — should be ~880+ tests now.

- [ ] **Step 5: Commit**

```bash
git add tests/test_integration_upsert.py tests/fixtures/companies_v1.csv tests/fixtures/companies_v2.csv tests/fixtures/companies_upsert_config.yaml
git commit -m "test: end-to-end upsert integration test"
```

---

### Task 17: Bump version and push branch

**Files:**
- Modify: `siphon/__init__.py`

- [ ] **Step 1: Bump version**

In `siphon/__init__.py`, change:

```python
__version__ = "0.2.0"
```

to:

```python
__version__ = "0.3.0a1"
```

(The `a1` denotes the first alpha of v0.3.x — the "production-grade ETL" series.)

- [ ] **Step 2: Run the full test suite once more**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: All pass.

- [ ] **Step 3: Commit and push**

```bash
git add siphon/__init__.py
git commit -m "chore: bump version to 0.3.0a1 (Phase 1 complete)"
git push -u origin v3-phase-1-upserts
```

- [ ] **Step 4: Verify on GitHub** that the branch shows up at `https://github.com/trojas-gnister/siphon/tree/v3-phase-1-upserts`.

---

## Verification

After all tasks complete:

1. **All tests pass:** `.venv/bin/pytest tests/ -q` shows zero failures.
2. **Coverage maintained:** `.venv/bin/pytest tests/ --cov=siphon` shows ≥95% coverage.
3. **Smoke test:** Manually run `siphon validate --config tests/fixtures/companies_upsert_config.yaml` and confirm "Config is valid".
4. **Branch pushed:** `v3-phase-1-upserts` exists on the remote.
5. **No proprietary references:** `grep -ri "workshield\|work.shield" --include="*.py" --include="*.yaml" --include="*.md"` returns no matches.

## Out of Scope

The following are deliberately deferred to later phases:

- **Diff reporting** in dry-run mode — Phase 2 will add this on top of upserts.
- **Audit trail** of which rows were inserted vs updated — Phase 4.
- **Batch-level commits** for resumability — Phase 3 changes the transaction model.
- **Unique constraint generation** when `--create-tables` is used and `on_conflict.key` is declared — could be added later, but for now users must declare unique constraints themselves (or use the existing tables they already have).

## Self-Review Notes

**Spec coverage check:**
- ✅ `OnConflictConfig` model with `key`, `action`, `update_columns` — Task 2
- ✅ Three actions (`update`, `skip`, `error`) — Tasks 9-11
- ✅ Dialect-agnostic via SQLAlchemy — Tasks 5-7
- ✅ PostgreSQL `on_conflict_do_update()` — Task 6
- ✅ SQLite native upsert — Task 5
- ✅ MySQL `on_duplicate_key_update()` — Task 7
- ✅ Generic select-then-update fallback with race-condition warning — Task 8
- ✅ Composite key support — Task 13
- ✅ Selective `update_columns` — Task 12
- ✅ Coexists with existing dedup (no changes needed; they operate at different stages)

**Type consistency check:**
- `OnConflictConfig` field names: `key`, `action`, `update_columns` — used consistently across Tasks 2, 3, 9-13.
- `build_upsert_statement` signature: `dialect`, `table`, `row`, `conflict_key`, `action`, `update_columns` — same in all dialect tasks.
- `GenericUpsertPlan` dataclass attributes: `table`, `row`, `conflict_key`, `action`, `update_columns` — used in Tasks 8, 10.

**Placeholder scan:** None found.
