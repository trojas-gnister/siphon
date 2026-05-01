# Siphon v3 Phase 6: Multi-Source Joins Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow Siphon to load multiple sources (e.g., one CSV per shard, or related data split across files), join them by a shared key, and feed the merged records through the existing validate → dedup → review → insert pipeline.

**Architecture:** A new top-level `sources:` config (plural) lets the user declare multiple named sources, each with its own loader, path, and field mappings. A `joins:` section describes how to combine them by a key. The pipeline loads each source independently, runs the mapper per source, then a new `Joiner` merges records into a flat list. Backward compatible: existing `source:` (singular) configs continue working unchanged.

**Tech Stack:** Python 3.11+, Pydantic 2.0, pytest, aiosqlite (test). No new external dependencies.

**Spec:** `docs/superpowers/specs/2026-04-22-siphon-v3-roadmap.md` (Phase 6 section)

**Branch:** Cut `v3-phase-6-multi-source-joins` from `v3-phase-4-audit-trail`.

---

## Config Shape

### Single-source (existing — unchanged)

```yaml
source:
  type: spreadsheet
schema:
  fields:
    - name: company_name
      source: "Name"
      type: string
      db: { table: companies, column: name }
  tables:
    companies: { primary_key: { column: id, type: auto_increment } }
```

### Multi-source (new)

```yaml
sources:
  - name: companies
    type: spreadsheet
    path: "./companies.csv"
    fields:
      - name: company_name
        source: "Name"
        type: string
        db: { table: companies, column: name }
      - name: company_code
        source: "Code"
        type: string

  - name: addresses
    type: spreadsheet
    path: "./addresses.csv"
    fields:
      - name: address
        source: "Street Address"
        type: string
        db: { table: addresses, column: full_address }
      - name: company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    on: company_code
    type: left          # left | inner

schema:
  tables:
    companies: { primary_key: { column: id, type: auto_increment } }
    addresses: { primary_key: { column: id, type: auto_increment } }
```

**Key design choices:**
1. Single source uses `source:` + top-level `schema.fields:` (existing).
2. Multi-source uses `sources:` (each with its own `fields:` and `path:`) + top-level `schema:` for tables/relationships/dedup.
3. Joins are processed in order: `joins[0]` produces a merged dataset that becomes the left side of `joins[1]`, etc.
4. Field names must be unique across sources participating in a join (so the merged record dict has no collisions). Validators enforce this.
5. The CLI `<input_path>` is required for single-source; ignored for multi-source (paths come from YAML).

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/config/schema.py` | Add `JoinConfig`, multi-source support, validators | Modify |
| `siphon/config/loader.py` | Cross-validate joins reference real sources | Modify |
| `siphon/core/joiner.py` | Merge records by join key (left/inner) | Create |
| `siphon/core/pipeline.py` | Multi-source orchestration: load → map per source → join → existing flow | Modify |
| `siphon/cli.py` | Make `input_path` optional when `sources:` is configured | Modify |
| `tests/config/test_schema_multi_source.py` | Schema tests for sources/joins | Create |
| `tests/core/test_joiner.py` | Unit tests for the Joiner | Create |
| `tests/core/test_pipeline_multi_source.py` | Pipeline tests with multi-source configs | Create |
| `tests/test_integration_multi_source.py` | End-to-end test with two CSVs and a left join | Create |

---

## Task Breakdown

### Task 1: Branch + baseline

**Files:** branch only

- [ ] **Step 1: Cut a new branch**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v3-phase-4-audit-trail
git pull
git checkout -b v3-phase-6-multi-source-joins
```

- [ ] **Step 2: Verify the v3.4 baseline**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `992 passed`.

- [ ] **Step 3: No commit yet** — branch creation alone doesn't need a commit.

---

### Task 2: Schema — JoinConfig and source-level fields/path

**Files:**
- Modify: `siphon/config/schema.py`
- Create: `tests/config/test_schema_multi_source.py`

Add a `JoinConfig` model and extend `SourceConfig` to allow optional `name`, `path`, and `fields` for the multi-source case. Add `sources` and `joins` to `SiphonConfig` and a validator enforcing exactly one of `source` or `sources`.

- [ ] **Step 1: Write failing tests**

Create `tests/config/test_schema_multi_source.py`:

```python
"""Tests for multi-source / join schema models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from siphon.config.schema import JoinConfig, SiphonConfig, SourceConfig


def _table(name: str = "t") -> dict:
    return {"primary_key": {"column": "id", "type": "auto_increment"}}


class TestJoinConfig:
    def test_left_join(self):
        j = JoinConfig(left="a", right="b", on="key", type="left")
        assert j.type == "left"
        assert j.left == "a"
        assert j.right == "b"
        assert j.on == "key"

    def test_inner_join(self):
        j = JoinConfig(left="a", right="b", on="key", type="inner")
        assert j.type == "inner"

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            JoinConfig(left="a", right="b", on="key", type="cross")

    def test_default_type_is_left(self):
        j = JoinConfig(left="a", right="b", on="key")
        assert j.type == "left"


class TestSourceConfigMultiSource:
    def test_source_config_accepts_name_path_fields(self):
        sc = SourceConfig(
            type="spreadsheet",
            name="companies",
            path="./companies.csv",
            fields=[{
                "name": "company_name",
                "source": "Name",
                "type": "string",
                "db": {"table": "companies", "column": "name"},
            }],
        )
        assert sc.name == "companies"
        assert sc.path == "./companies.csv"
        assert sc.fields is not None
        assert len(sc.fields) == 1


class TestSiphonConfigMultiSource:
    def test_single_source_config_unchanged(self):
        """Existing source: + schema.fields: shape still works."""
        cfg = SiphonConfig.model_validate({
            "name": "single",
            "source": {"type": "spreadsheet"},
            "database": {"url": "sqlite:///t.db"},
            "schema": {
                "fields": [{
                    "name": "n", "source": "N", "type": "string",
                    "db": {"table": "t", "column": "n"},
                }],
                "tables": {"t": _table()},
            },
        })
        assert cfg.source is not None
        assert cfg.sources is None or cfg.sources == []

    def test_multi_source_config(self):
        """sources: list with per-source fields and a join."""
        cfg = SiphonConfig.model_validate({
            "name": "multi",
            "sources": [
                {
                    "name": "companies",
                    "type": "spreadsheet",
                    "path": "./companies.csv",
                    "fields": [{
                        "name": "company_name", "source": "Name", "type": "string",
                        "db": {"table": "companies", "column": "name"},
                    }, {
                        "name": "company_code", "source": "Code", "type": "string",
                    }],
                },
                {
                    "name": "addresses",
                    "type": "spreadsheet",
                    "path": "./addresses.csv",
                    "fields": [{
                        "name": "address", "source": "Address", "type": "string",
                        "db": {"table": "addresses", "column": "full_address"},
                    }, {
                        "name": "company_code", "source": "Company Code", "type": "string",
                    }],
                },
            ],
            "joins": [
                {"left": "companies", "right": "addresses", "on": "company_code"},
            ],
            "database": {"url": "sqlite:///t.db"},
            "schema": {
                "tables": {
                    "companies": _table(),
                    "addresses": _table(),
                },
            },
        })
        assert cfg.sources is not None
        assert len(cfg.sources) == 2
        assert cfg.joins is not None
        assert len(cfg.joins) == 1
        assert cfg.joins[0].on == "company_code"

    def test_both_source_and_sources_rejected(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "source": {"type": "spreadsheet"},
                "sources": [{
                    "name": "x", "type": "spreadsheet", "path": "./x.csv",
                    "fields": [{
                        "name": "a", "source": "A", "type": "string",
                        "db": {"table": "t", "column": "a"},
                    }],
                }],
                "database": {"url": "sqlite:///t.db"},
                "schema": {
                    "fields": [],
                    "tables": {"t": _table()},
                },
            })

    def test_neither_source_nor_sources_rejected(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "database": {"url": "sqlite:///t.db"},
                "schema": {
                    "fields": [],
                    "tables": {"t": _table()},
                },
            })

    def test_sources_must_have_name(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "sources": [{
                    "type": "spreadsheet", "path": "./x.csv",
                    "fields": [],
                }],
                "database": {"url": "sqlite:///t.db"},
                "schema": {"tables": {"t": _table()}},
            })

    def test_sources_must_have_fields(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "sources": [{
                    "name": "x", "type": "spreadsheet", "path": "./x.csv",
                }],
                "database": {"url": "sqlite:///t.db"},
                "schema": {"tables": {"t": _table()}},
            })

    def test_sources_with_unique_names(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "sources": [
                    {"name": "x", "type": "spreadsheet", "path": "./a.csv", "fields": [{
                        "name": "a", "source": "A", "type": "string",
                        "db": {"table": "t", "column": "a"},
                    }]},
                    {"name": "x", "type": "spreadsheet", "path": "./b.csv", "fields": [{
                        "name": "b", "source": "B", "type": "string",
                        "db": {"table": "t", "column": "b"},
                    }]},
                ],
                "database": {"url": "sqlite:///t.db"},
                "schema": {"tables": {"t": _table()}},
            })
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/config/test_schema_multi_source.py -v`
Expected: FAIL — `JoinConfig` and multi-source fields don't exist.

- [ ] **Step 3: Add `JoinConfig`**

In `siphon/config/schema.py`, add this class before `SiphonConfig`:

```python
class JoinConfig(BaseModel):
    """Describes how to merge two named sources by a shared key."""

    model_config = ConfigDict(populate_by_name=True)

    left: str
    right: str
    on: str
    type: Literal["left", "inner"] = "left"
```

- [ ] **Step 4: Extend `SourceConfig` with optional name/path/fields**

Find the existing `SourceConfig`. Add these fields:

```python
class SourceConfig(BaseModel):
    """Source data configuration."""
    model_config = ConfigDict(populate_by_name=True)

    type: Literal["spreadsheet", "xml", "json"]
    root: str | None = None
    encoding: str = "utf-8"
    force_list: list[str] | None = None
    # Multi-source fields (required when used inside a sources: list)
    name: str | None = None
    path: str | None = None
    fields: list["FieldConfig"] | None = None
```

The `"FieldConfig"` forward reference is needed if `FieldConfig` is defined later in the file. If it's defined earlier, drop the quotes.

- [ ] **Step 5: Add `sources` and `joins` to `SiphonConfig`**

Find `SiphonConfig`. Add:

```python
class SiphonConfig(BaseModel):
    name: str
    source: SourceConfig | None = None
    sources: list[SourceConfig] | None = None
    joins: list[JoinConfig] | None = None
    database: DatabaseConfig
    schema_: SchemaConfig = Field(alias="schema")
    transforms: TransformFileConfig | None = None
    variables: dict[str, Any] | None = None
    relationships: list[Relationship] = []
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
```

NOTE: `source` was previously required. Make it optional now and rely on the validator below to enforce that exactly one of `source` or `sources` is set.

- [ ] **Step 6: Add the cross-validator**

Add a `model_validator(mode="after")` to `SiphonConfig`. Append after the existing `cross_validate_references` validator:

```python
    @model_validator(mode="after")
    def validate_source_form(self) -> "SiphonConfig":
        """Enforce exactly one of `source` (singular) or `sources` (plural)."""
        has_single = self.source is not None
        has_multi = self.sources is not None and len(self.sources) > 0

        if has_single and has_multi:
            raise ValueError(
                "Config must use either `source:` or `sources:`, not both."
            )
        if not has_single and not has_multi:
            raise ValueError(
                "Config must declare either `source:` or `sources:`."
            )

        if has_multi:
            # Each source must declare name + fields when in a list
            seen_names = set()
            for src in self.sources:
                if not src.name:
                    raise ValueError(
                        "Each source in `sources:` must declare a `name`."
                    )
                if src.name in seen_names:
                    raise ValueError(
                        f"Duplicate source name '{src.name}' in `sources:`."
                    )
                seen_names.add(src.name)
                if not src.fields:
                    raise ValueError(
                        f"Source '{src.name}' must declare `fields:`."
                    )

        return self
```

- [ ] **Step 7: Run tests**

Run: `.venv/bin/pytest tests/config/test_schema_multi_source.py -v`
Expected: 11 passed (4 + 1 + 6 across the test classes).

- [ ] **Step 8: Run full schema suite for regressions**

Run: `.venv/bin/pytest tests/config/ -q`
Expected: All config tests pass.

- [ ] **Step 9: Commit**

```bash
git add siphon/config/schema.py tests/config/test_schema_multi_source.py
git commit -m "feat: JoinConfig + multi-source schema (sources, joins)"
```

---

### Task 3: Loader — cross-validate joins reference real sources

**Files:**
- Modify: `siphon/config/loader.py`
- Modify: `tests/config/test_loader.py`

The loader must check that every `joins[].left` and `joins[].right` references either a declared source name OR the result of a previous join (chained joins).

For v3.6 simplicity: only validate that each join's `left` and `right` are source names that exist in `sources`. Chained joins (where one join's output feeds another) are out of scope; if needed, users can use a separate intermediate flow.

Wait — that's too restrictive. The plan should support multiple joins. Let me reconsider:
- If user has 3 sources A, B, C and wants to join all three, the joins list has two entries.
- After `joins[0]: left=A, right=B` produces a merged dataset, the user might want `joins[1]: left=AB_result, right=C`.

For simplicity in v3.6: every `left` and `right` must be a source name (no intermediate names). The pipeline processes joins sequentially, accumulating the result by treating the previous join output as the new "left side" for the next join — but only if `joins[N].left` matches `joins[N-1].left` (i.e., we're always extending the same starting source).

This is restrictive but covers the common case (one base source enriched by N lookup sources). Document the limitation.

- [ ] **Step 1: Write failing tests**

Append to `tests/config/test_loader.py`:

```python
def test_join_unknown_source_raises(tmp_path):
    yaml = """
name: bad
sources:
  - name: a
    type: spreadsheet
    path: "./a.csv"
    fields:
      - name: x
        source: "X"
        type: string
        db: { table: t, column: x }
joins:
  - left: a
    right: nonexistent
    on: x
database: { url: "sqlite:///t.db" }
schema:
  tables:
    t: { primary_key: { column: id, type: auto_increment } }
pipeline: { review: false }
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    from siphon.utils.errors import ConfigError
    from siphon.config.loader import load_config
    with pytest.raises(ConfigError, match="join.*nonexistent"):
        load_config(p)


def test_join_with_valid_sources_loads(tmp_path):
    yaml = """
name: ok
sources:
  - name: a
    type: spreadsheet
    path: "./a.csv"
    fields:
      - name: x
        source: "X"
        type: string
        db: { table: t, column: x }
  - name: b
    type: spreadsheet
    path: "./b.csv"
    fields:
      - name: y
        source: "Y"
        type: string
        db: { table: t, column: y }
joins:
  - left: a
    right: b
    on: x
database: { url: "sqlite:///t.db" }
schema:
  tables:
    t: { primary_key: { column: id, type: auto_increment } }
pipeline: { review: false }
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    from siphon.config.loader import load_config
    cfg = load_config(p)
    assert cfg.joins is not None
    assert cfg.joins[0].left == "a"
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/config/test_loader.py::test_join_unknown_source_raises tests/config/test_loader.py::test_join_with_valid_sources_loads -v`
Expected: First fails (no error raised).

- [ ] **Step 3: Add validation to `siphon/config/loader.py`**

Find the `_cross_validate` function. After existing validations, add:

```python
    # Validate join.left and join.right reference real sources
    if config.sources and config.joins:
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
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/pytest tests/config/test_loader.py -v 2>&1 | tail -10`
Expected: All loader tests pass, including the 2 new ones.

- [ ] **Step 5: Commit**

```bash
git add siphon/config/loader.py tests/config/test_loader.py
git commit -m "feat: cross-validate join.left and join.right against declared sources"
```

---

### Task 4: Joiner — merge records by key

**Files:**
- Create: `siphon/core/joiner.py`
- Create: `tests/core/test_joiner.py`

The `Joiner` is a pure function over already-mapped records. It doesn't know about sources or DB; it just merges two flat dict lists by a shared key.

- [ ] **Step 1: Write failing tests**

Create `tests/core/test_joiner.py`:

```python
"""Tests for the Joiner — merge two record lists by a shared key."""

from __future__ import annotations

from siphon.core.joiner import join_records


class TestLeftJoin:
    def test_one_to_one_merges_all_fields(self):
        left = [{"company_code": "A", "company_name": "Acme"}]
        right = [{"company_code": "A", "address": "123 Main"}]
        result = join_records(left, right, on="company_code", join_type="left")
        assert result == [{
            "company_code": "A",
            "company_name": "Acme",
            "address": "123 Main",
        }]

    def test_unmatched_left_keeps_left_fields_with_none_for_right(self):
        left = [
            {"company_code": "A", "company_name": "Acme"},
            {"company_code": "B", "company_name": "Beta"},
        ]
        right = [{"company_code": "A", "address": "1"}]
        result = join_records(left, right, on="company_code", join_type="left")
        assert len(result) == 2
        beta = next(r for r in result if r["company_code"] == "B")
        # Right fields filled with None for unmatched left rows
        assert beta == {"company_code": "B", "company_name": "Beta", "address": None}

    def test_one_to_many_produces_one_row_per_right_match(self):
        left = [{"company_code": "A", "company_name": "Acme"}]
        right = [
            {"company_code": "A", "address": "1"},
            {"company_code": "A", "address": "2"},
        ]
        result = join_records(left, right, on="company_code", join_type="left")
        assert len(result) == 2
        addresses = sorted(r["address"] for r in result)
        assert addresses == ["1", "2"]
        assert all(r["company_name"] == "Acme" for r in result)

    def test_empty_right_keeps_all_left_with_none_added(self):
        left = [{"company_code": "A", "company_name": "Acme"}]
        right = []
        result = join_records(left, right, on="company_code", join_type="left")
        assert result == [{"company_code": "A", "company_name": "Acme"}]

    def test_empty_left_returns_empty(self):
        left = []
        right = [{"company_code": "A", "address": "1"}]
        result = join_records(left, right, on="company_code", join_type="left")
        assert result == []


class TestInnerJoin:
    def test_only_matched_rows_returned(self):
        left = [
            {"k": "A", "x": 1},
            {"k": "B", "x": 2},
        ]
        right = [{"k": "A", "y": 10}]
        result = join_records(left, right, on="k", join_type="inner")
        assert result == [{"k": "A", "x": 1, "y": 10}]

    def test_no_matches_returns_empty(self):
        left = [{"k": "A", "x": 1}]
        right = [{"k": "B", "y": 10}]
        result = join_records(left, right, on="k", join_type="inner")
        assert result == []


class TestKeyHandling:
    def test_missing_key_in_left_treated_as_none(self):
        """A left row with no value at the join key never matches."""
        left = [{"x": 1}]  # no 'k'
        right = [{"k": "A", "y": 10}]
        result = join_records(left, right, on="k", join_type="left")
        # Left join: left row is preserved, right fields are None
        assert len(result) == 1
        assert result[0]["x"] == 1
        assert result[0].get("y") is None

    def test_unknown_join_type_raises(self):
        import pytest
        with pytest.raises(ValueError, match="join_type"):
            join_records([], [], on="k", join_type="cross")

    def test_left_fields_take_precedence_on_key_overlap(self):
        """If left and right both have the same NON-KEY column, left wins."""
        left = [{"k": "A", "shared": "from_left"}]
        right = [{"k": "A", "shared": "from_right"}]
        result = join_records(left, right, on="k", join_type="left")
        assert result[0]["shared"] == "from_left"
```

- [ ] **Step 2: Verify tests fail**

Run: `.venv/bin/pytest tests/core/test_joiner.py -v`
Expected: ImportError on `siphon.core.joiner`.

- [ ] **Step 3: Implement the Joiner**

Create `siphon/core/joiner.py`:

```python
"""Join two record lists by a shared key.

The Joiner is a pure function over flat dict lists — no DB, no I/O.
Used by the pipeline after each source's records have been mapped.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def join_records(
    left: list[dict],
    right: list[dict],
    *,
    on: str,
    join_type: str = "left",
) -> list[dict]:
    """Merge two record lists by a shared key.

    Args:
        left: List of left-side record dicts.
        right: List of right-side record dicts.
        on: The key name present in both sides' records.
        join_type: "left" (keep all left rows) or "inner" (only matched rows).

    Returns:
        Merged list of dicts. Each merged dict has all left fields plus all
        right fields. If both sides have a non-key column with the same name,
        the LEFT side's value wins.
    """
    if join_type not in ("left", "inner"):
        raise ValueError(
            f"Unknown join_type: '{join_type}'. Must be 'left' or 'inner'."
        )

    # Index right side by key
    right_index: dict[Any, list[dict]] = defaultdict(list)
    for r in right:
        key_val = r.get(on)
        if key_val is not None:
            right_index[key_val].append(r)

    merged: list[dict] = []

    for left_row in left:
        key_val = left_row.get(on)
        right_matches = right_index.get(key_val) if key_val is not None else None

        if not right_matches:
            if join_type == "inner":
                continue
            # Left join: keep left row, fill right fields with None
            merged.append(_merge_with_nones(left_row, right))
        else:
            for r in right_matches:
                merged.append(_merge(left_row, r))

    return merged


def _merge(left_row: dict, right_row: dict) -> dict:
    """Merge two dicts; left fields take precedence on overlap."""
    return {**right_row, **left_row}


def _merge_with_nones(left_row: dict, right_sample: list[dict]) -> dict:
    """Build a left-join row when no right match exists.

    Right fields are filled with None based on the columns observed in the
    right-side sample.
    """
    if not right_sample:
        return dict(left_row)
    right_columns: set[str] = set()
    for r in right_sample:
        right_columns.update(r.keys())
    null_right = {col: None for col in right_columns if col not in left_row}
    return {**left_row, **null_right}
```

- [ ] **Step 4: Run tests**

Run: `.venv/bin/pytest tests/core/test_joiner.py -v`
Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add siphon/core/joiner.py tests/core/test_joiner.py
git commit -m "feat: Joiner — merge record lists by shared key (left, inner)"
```

---

### Task 5: Pipeline — multi-source orchestration

**Files:**
- Modify: `siphon/core/pipeline.py`
- Create: `tests/core/test_pipeline_multi_source.py`

The pipeline currently loads from a single source. For multi-source, it must:

1. Determine if config uses `source:` (single) or `sources:` (multi).
2. For multi: load each source via its loader using the source's `path`. Run the mapper per source using the source's `fields`. Apply joins in order, accumulating the merged dataset.
3. For single: existing behavior unchanged.
4. Hand the final flat record list to the existing validator → dedup → review → insert pipeline.

The validator and downstream stages need a single source-of-truth list of fields. For multi-source, build that by concatenating each source's `fields:`.

- [ ] **Step 1: Add a helper to compute effective fields for the schema**

The existing `Validator` and others read `config.schema_.fields`. For multi-source, that's empty — fields live in each `SourceConfig`. We need a flat view.

Add a helper to `Pipeline` that returns the flat list of fields used by validation/dedup/insertion. In multi-source mode, concatenate. In single-source, return existing.

In `siphon/core/pipeline.py`, add this method:

```python
    def _effective_fields(self):
        """Return the flat list of FieldConfig used by validate/dedup/insert.

        - Single-source: config.schema_.fields
        - Multi-source: concatenation of each source's fields (in source order)
        """
        if self._config.sources:
            flat = []
            for src in self._config.sources:
                if src.fields:
                    flat.extend(src.fields)
            return flat
        return self._config.schema_.fields
```

- [ ] **Step 2: Update the Validator/ModelGenerator/Inserter to use effective fields**

The validator and inserter read `self._config.schema_.fields` directly today. The cleanest fix: when building the pipeline's components, mutate `self._config.schema_.fields` to be the flat list before constructing them. Since `SiphonConfig` is mutable (it's a Pydantic model), we can assign:

```python
        # Normalize fields: ensure schema_.fields holds the flat view
        if self._config.sources:
            self._config.schema_.fields = self._effective_fields()
```

Place this at the very start of `run()`, before any component is constructed.

This keeps Validator, ModelGenerator, and Inserter unchanged. They continue reading `config.schema_.fields` and just see the flat union view in multi-source mode.

- [ ] **Step 3: Write failing pipeline tests**

Create `tests/core/test_pipeline_multi_source.py`:

```python
"""Pipeline tests for multi-source loading + joins."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _csv(path: Path, headers: list[str], rows: list[list[str]]) -> Path:
    lines = [",".join(headers)]
    for r in rows:
        lines.append(",".join(r))
    path.write_text("\n".join(lines) + "\n")
    return path


def _yaml(tmp_path: Path, db_path: Path,
          companies_csv: Path, addresses_csv: Path,
          join_type: str = "left") -> Path:
    yaml = f"""
name: multi-source
sources:
  - name: companies
    type: spreadsheet
    path: "{companies_csv}"
    fields:
      - name: company_name
        source: "Name"
        type: string
        required: true
        db: {{ table: companies, column: name }}
      - name: company_code
        source: "Code"
        type: string

  - name: addresses
    type: spreadsheet
    path: "{addresses_csv}"
    fields:
      - name: address
        source: "Address"
        type: string
        db: {{ table: addresses, column: full_address }}
      - name: addr_company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    on: company_code
    type: {join_type}

database: {{ url: "sqlite+aiosqlite:///{db_path}" }}

schema:
  tables:
    companies: {{ primary_key: {{ column: id, type: auto_increment }} }}
    addresses: {{ primary_key: {{ column: id, type: auto_increment }} }}

pipeline:
  review: false
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


async def _query(db_url: str, sql: str):
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                return result.fetchall()
            return []
    finally:
        await engine.dispose()


class TestMultiSourceLoad:
    async def test_left_join_loads_both_sources_and_inserts(self, tmp_path):
        # NOTE: addresses CSV joins on Company Code; left join keeps a company
        # with no address.
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"], ["Beta", "B001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["123 Main", "A001"]])  # No address for Beta
        db_path = tmp_path / "test.db"
        config_path = _yaml(tmp_path, db_path, comp, addr, join_type="left")

        config = load_config(config_path)
        # No CLI input path — multi-source uses YAML paths
        result = await Pipeline(config).run(
            input_path=str(comp),  # placeholder; multi-source ignores this
            no_review=True,
            create_tables=True,
        )

        assert result.total_extracted == 2  # 2 companies post-join
        assert result.total_inserted >= 1  # at least Acme

        rows = await _query(f"sqlite+aiosqlite:///{db_path}",
                            "SELECT name FROM companies ORDER BY name")
        names = [r[0] for r in rows]
        assert "Acme" in names
        assert "Beta" in names

        addr_rows = await _query(f"sqlite+aiosqlite:///{db_path}",
                                 "SELECT full_address FROM addresses")
        addresses = [r[0] for r in addr_rows]
        assert "123 Main" in addresses

    async def test_inner_join_drops_unmatched_left_rows(self, tmp_path):
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"], ["Beta", "B001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["123 Main", "A001"]])  # No Beta address
        db_path = tmp_path / "test.db"
        config_path = _yaml(tmp_path, db_path, comp, addr, join_type="inner")

        config = load_config(config_path)
        result = await Pipeline(config).run(
            input_path=str(comp),
            no_review=True, create_tables=True,
        )

        # Inner join: only Acme survives
        assert result.total_extracted == 1

    async def test_one_to_many_produces_multiple_addresses(self, tmp_path):
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["1", "A001"], ["2", "A001"], ["3", "A001"]])
        db_path = tmp_path / "test.db"
        config_path = _yaml(tmp_path, db_path, comp, addr, join_type="left")

        config = load_config(config_path)
        result = await Pipeline(config).run(
            input_path=str(comp), no_review=True, create_tables=True,
        )

        assert result.total_extracted == 3  # 3 merged rows (one per address)

        addr_rows = await _query(f"sqlite+aiosqlite:///{db_path}",
                                 "SELECT full_address FROM addresses ORDER BY full_address")
        assert [r[0] for r in addr_rows] == ["1", "2", "3"]
```

- [ ] **Step 4: Verify tests fail**

Run: `.venv/bin/pytest tests/core/test_pipeline_multi_source.py -v`
Expected: FAIL — pipeline doesn't handle multi-source yet.

- [ ] **Step 5: Modify the pipeline**

Read the current `siphon/core/pipeline.py` and find the section that loads source data. It looks something like:

```python
        # Load source data
        if source_config.type == "spreadsheet":
            loader = SpreadsheetLoader()
            ...
        elif source_config.type == "xml":
            ...
```

Replace that single-source block with a dispatch on `self._config.sources` vs `self._config.source`.

(a) Add this helper method to the `Pipeline` class:

```python
    def _load_and_map_source(self, src, mapper_for_src):
        """Load and map records from a single named source. Synchronous helper."""
        # The actual loading is async; this is split below.
        raise NotImplementedError("see _load_source / _map_source below")

    async def _load_source(self, src):
        """Load raw records from a single SourceConfig (with its own path)."""
        from siphon.sources.spreadsheet import SpreadsheetLoader
        from siphon.sources.xml import XMLLoader

        if src.type == "spreadsheet":
            loader = SpreadsheetLoader()
            if not src.path:
                raise ConfigError(
                    f"Source '{src.name}' has no path declared"
                )
            return loader.load(src.path)
        elif src.type == "xml":
            loader = XMLLoader(
                root=src.root,
                encoding=src.encoding,
                force_list=src.force_list,
            )
            if not src.path:
                raise ConfigError(
                    f"Source '{src.name}' has no path declared"
                )
            return loader.load(src.path)
        else:
            raise ConfigError(f"Unsupported source type: {src.type}")
```

(b) In `run()`, replace the existing source-loading block. Find the section (it loads + maps from `self._config.source`) and replace with:

```python
        # 1. Load custom transforms (unchanged)
        custom_transforms = {}
        if self._config.transforms and self._config.transforms.file:
            custom_transforms = load_custom_transforms(self._config.transforms.file)

        # Multi-source: load each, map each, then join
        if self._config.sources:
            from siphon.core.joiner import join_records

            # Normalize: collapse all sources' fields into schema_.fields so
            # downstream validator/inserter can work unchanged.
            self._config.schema_.fields = self._effective_fields()

            # Load + map each source independently
            mapped_per_source: dict[str, list[dict]] = {}
            for src in self._config.sources:
                raw = await self._load_source(src)
                # Build a per-source temporary config for the Mapper that has
                # only this source's fields.
                src_mapper = self._build_source_mapper(src, custom_transforms)
                mapped_per_source[src.name] = src_mapper.map_records(raw)

            # Apply joins in order. The first join's left becomes the seed.
            if self._config.joins:
                # Start with the first join's left source
                first_left = self._config.joins[0].left
                merged = list(mapped_per_source[first_left])
                for j in self._config.joins:
                    if j.left == first_left or j.left == "_merged":
                        # extending the running merged set
                        right_records = mapped_per_source[j.right]
                        merged = join_records(
                            merged, right_records,
                            on=j.on, join_type=j.type,
                        )
                    else:
                        # different left source: produce a separate merge and
                        # extend the merged list with it
                        side_merge = join_records(
                            mapped_per_source[j.left],
                            mapped_per_source[j.right],
                            on=j.on, join_type=j.type,
                        )
                        merged.extend(side_merge)
                records = merged
            else:
                # No joins declared: union all sources' records
                records = []
                for src_records in mapped_per_source.values():
                    records.extend(src_records)

            # Empty collections result for multi-source (collections only
            # supported in single-source XML/JSON)
            all_collection_records = {}
        else:
            # Single-source path — existing behaviour, unchanged.
            # ... (keep the existing single-source block here)
            ...
```

The exact placement and wrapping of the single-source path depends on the existing structure. Read the file and integrate carefully — keep the existing single-source flow intact inside the `else` branch.

(c) Add the `_build_source_mapper` helper:

```python
    def _build_source_mapper(self, src, custom_transforms: dict):
        """Build a Mapper that operates only on this source's fields.

        Creates a transient SiphonConfig view with `schema_.fields = src.fields`
        so the existing Mapper class works unchanged.
        """
        from copy import copy
        from siphon.core.mapper import Mapper

        # Shallow copy the config, override schema_.fields
        cfg_view = copy(self._config)
        # We need a separate schema_ so we can mutate fields without affecting
        # the global config. Pydantic models support model_copy.
        cfg_view = self._config.model_copy(deep=True)
        cfg_view.schema_.fields = list(src.fields or [])
        # Drop collections in this view — collections are only supported on
        # single-source XML/JSON.
        cfg_view.schema_.collections = None

        return Mapper(cfg_view, custom_transforms)
```

- [ ] **Step 6: Run new tests**

Run: `.venv/bin/pytest tests/core/test_pipeline_multi_source.py -v`
Expected: 3 passed.

- [ ] **Step 7: Run full suite for regressions**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add siphon/core/pipeline.py tests/core/test_pipeline_multi_source.py
git commit -m "feat: pipeline orchestrates multi-source loading + joins"
```

---

### Task 6: CLI — make input_path optional for multi-source

**Files:**
- Modify: `siphon/cli.py`
- Modify: `tests/test_cli.py`

In multi-source mode, the YAML declares each source's path. The CLI's `input_path` argument becomes optional. We can't easily check the config from argument parsing, so the simplest UX:

- Allow `siphon run` (no positional arg) when the config uses `sources:`.
- Keep `siphon run <input_path>` working for single-source.
- If `input_path` is given but the config uses `sources:`, log a warning and ignore it.

Typer makes `Argument(...)` required by default. Change it to optional via `Argument(None, ...)`.

- [ ] **Step 1: Write failing test**

Append to `tests/test_cli.py`:

```python
class TestMultiSourceCLI:
    def test_run_without_input_path_works_with_sources_config(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        # Build a config that uses `sources:` (no top-level source/fields)
        config_yaml = """
name: multi
sources:
  - name: a
    type: spreadsheet
    path: "./a.csv"
    fields:
      - name: x
        source: "X"
        type: string
        required: true
        db: { table: t, column: x }
database: { url: "sqlite:///t.db" }
schema:
  tables:
    t: { primary_key: { column: id, type: auto_increment } }
pipeline: { review: false }
"""
        config_file = tmp_path / "siphon.yaml"
        config_file.write_text(config_yaml)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = [MagicMock()]  # truthy
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            # Invoke without a positional input path
            result = runner.invoke(app, [
                "run", "--config", str(config_file), "--no-review",
            ])

        # Should not error out for missing input_path
        assert result.exit_code == 0
```

- [ ] **Step 2: Verify the test fails**

Run: `.venv/bin/pytest tests/test_cli.py::TestMultiSourceCLI::test_run_without_input_path_works_with_sources_config -v`
Expected: FAIL — Typer rejects missing positional argument.

- [ ] **Step 3: Make `input_path` optional in `siphon/cli.py`**

Find the `run` command's `input_path` argument. Change:

```python
    input_path: str = typer.Argument(..., help="Path to spreadsheet file or directory"),
```

to:

```python
    input_path: Optional[str] = typer.Argument(
        None, help="Path to spreadsheet file or directory (required for single-source configs)"
    ),
```

After loading the config, validate the combination:

```python
    cfg = load_config(config)

    if cfg.sources is None and not input_path:
        console.print("[red]Error:[/red] input_path is required when not using a multi-source (`sources:`) config")
        raise typer.Exit(code=1)
    
    if cfg.sources is not None and input_path:
        console.print(
            "[yellow]Warning:[/yellow] input_path ignored — multi-source configs use paths from YAML"
        )
        input_path = None
```

In the `pipeline.run()` call, accept the `input_path or ""` (or however `Pipeline.run()` already handles it). Read the existing call to see the parameter name and adjust:

```python
        result = asyncio.run(
            pipeline.run(
                input_path or "",
                # ... existing kwargs
            )
        )
```

NOTE: `Pipeline.run()` currently expects a non-empty input_path for single-source. For multi-source, the path is unused inside the pipeline (each source has its own). So passing an empty string should be fine — but verify by checking the current `Pipeline.run()` signature/body. If it requires a real path, gate the value: pass `""` when sources is set.

- [ ] **Step 4: Run new test**

Run: `.venv/bin/pytest tests/test_cli.py::TestMultiSourceCLI -v`
Expected: 1 passed.

- [ ] **Step 5: Run full CLI suite**

Run: `.venv/bin/pytest tests/test_cli.py -v 2>&1 | tail -10`
Expected: All pass.

- [ ] **Step 6: Run full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 7: Commit**

```bash
git add siphon/cli.py tests/test_cli.py
git commit -m "feat: make input_path optional for multi-source configs"
```

---

### Task 7: End-to-end multi-source integration test

**Files:**
- Create: `tests/test_integration_multi_source.py`

A real test with two CSVs, a left join, and verification that records flow into multiple tables correctly.

- [ ] **Step 1: Write the test**

Create `tests/test_integration_multi_source.py`:

```python
"""End-to-end test for multi-source pipelines with joins."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


async def _query(db_url: str, sql: str):
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                return result.fetchall()
            return []
    finally:
        await engine.dispose()


def _write_companies(path: Path) -> Path:
    path.write_text(
        "Name,Code\n"
        "Acme,A001\n"
        "Beta,B001\n"
        "Gamma,G001\n"
    )
    return path


def _write_addresses(path: Path) -> Path:
    path.write_text(
        "Address,Company Code,State\n"
        "123 Main,A001,CA\n"
        "456 Oak,A001,CA\n"
        "789 Pine,B001,NY\n"
    )
    return path


def _write_yaml(tmp_path: Path, db_path: Path,
                companies_csv: Path, addresses_csv: Path) -> Path:
    yaml = f"""
name: e2e-multi-source
sources:
  - name: companies
    type: spreadsheet
    path: "{companies_csv}"
    fields:
      - name: company_name
        source: "Name"
        type: string
        required: true
        min_length: 2
        db: {{ table: companies, column: name }}
      - name: company_code
        source: "Code"
        type: string

  - name: addresses
    type: spreadsheet
    path: "{addresses_csv}"
    fields:
      - name: address
        source: "Address"
        type: string
        db: {{ table: addresses, column: full_address }}
      - name: state
        source: "State"
        type: enum
        preset: us_states
        db: {{ table: addresses, column: state_code }}
      - name: addr_company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    on: company_code
    type: left

database: {{ url: "sqlite+aiosqlite:///{db_path}" }}

schema:
  tables:
    companies: {{ primary_key: {{ column: id, type: auto_increment }} }}
    addresses: {{ primary_key: {{ column: id, type: auto_increment }} }}
  deduplication:
    key: [company_name]
    check_db: false
    match: case_insensitive

pipeline:
  review: false
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


class TestMultiSourceEndToEnd:
    async def test_two_csvs_join_and_insert_into_multiple_tables(self, tmp_path):
        comp = _write_companies(tmp_path / "companies.csv")
        addr = _write_addresses(tmp_path / "addresses.csv")
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path, comp, addr)

        config = load_config(config_path)
        result = await Pipeline(config).run(
            input_path="",  # ignored for multi-source
            no_review=True,
            create_tables=True,
        )

        # Acme has 2 addresses → 2 merged rows
        # Beta has 1 address  → 1 merged row
        # Gamma has 0 addresses → 1 merged row (left join)
        # Total extracted = 4
        assert result.total_extracted == 4

        # Companies: Acme deduped to 1 row (case_insensitive, key=company_name);
        # Beta and Gamma each unique → 3 rows total
        company_rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT name FROM companies ORDER BY name",
        )
        assert [r[0] for r in company_rows] == ["Acme", "Beta", "Gamma"]

        # Addresses: 3 inserted (Acme×2, Beta×1; Gamma had no address)
        addr_rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT full_address, state_code FROM addresses ORDER BY full_address",
        )
        addresses = [(r[0], r[1]) for r in addr_rows]
        assert addresses == [
            ("123 Main", "CA"),
            ("456 Oak", "CA"),
            ("789 Pine", "NY"),
        ]
```

- [ ] **Step 2: Run the test**

Run: `.venv/bin/pytest tests/test_integration_multi_source.py -v`
Expected: 1 passed.

- [ ] **Step 3: Run full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 4: Commit**

```bash
git add tests/test_integration_multi_source.py
git commit -m "test: end-to-end multi-source join integration test"
```

---

### Task 8: README + version bump + push

**Files:**
- Modify: `README.md`
- Modify: `siphon/__init__.py`

- [ ] **Step 1: Add a "Multi-Source Joins" section to `README.md`**

Find the existing "## Audit Trail" section. Insert the new section IMMEDIATELY AFTER it (before the next `##` heading). Use literal triple backticks in the actual file:

```markdown
## Multi-Source Joins

When data is split across multiple files (e.g., one CSV per shard, denormalized exports, or a primary file plus enrichment lookups), use `sources:` to declare each input and `joins:` to combine them.

```yaml
sources:
  - name: companies
    type: spreadsheet
    path: "./companies.csv"
    fields:
      - name: company_name
        source: "Name"
        type: string
        db: { table: companies, column: name }
      - name: company_code
        source: "Code"
        type: string

  - name: addresses
    type: spreadsheet
    path: "./addresses.csv"
    fields:
      - name: address
        source: "Street Address"
        type: string
        db: { table: addresses, column: full_address }
      - name: company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    on: company_code
    type: left          # left | inner

schema:
  tables:
    companies: { primary_key: { column: id, type: auto_increment } }
    addresses: { primary_key: { column: id, type: auto_increment } }
```

**Join types:**
- `left` (default) — keep all left rows; right fields are NULL where no match exists
- `inner` — drop left rows that have no matching right row

**Multiple joins:** Process in order. The first join's output becomes the running merged dataset; later joins extend it. Useful for one base source enriched by N lookup sources.

**Path handling:** When using `sources:`, each source declares its own `path:` in YAML. The CLI argument `siphon run <input_path>` is optional and ignored. Paths support `${ENV_VAR}` substitution.

**Cardinality:** A 1-to-many join produces N merged rows (one per right match). Configure `deduplication.key:` on the left-side fields if you want one row per distinct key in the target tables.

**Backward compatible:** Single-source configs (`source:` + top-level `schema.fields:`) continue to work unchanged.
```

- [ ] **Step 2: Bump version**

In `siphon/__init__.py`, change:

```python
__version__ = "0.3.0a4"
```

to:

```python
__version__ = "0.3.0a5"
```

- [ ] **Step 3: Run the full suite**

Run: `.venv/bin/pytest tests/ -q 2>&1 | tail -3`
Expected: All pass.

- [ ] **Step 4: Audit for proprietary references**

Run: `grep -ri "workshield\|work.shield" --include="*.py" --include="*.yaml" --include="*.md" --include="*.toml" .`
Expected: No matches in production code (matches in `docs/superpowers/plans/` are command quotations and can be ignored).

- [ ] **Step 5: Commit and push**

```bash
git add README.md siphon/__init__.py
git commit -m "chore: bump version to 0.3.0a5 (Phase 6 complete)"
git push -u origin v3-phase-6-multi-source-joins
```

---

## Verification

After all tasks complete:
1. All tests pass: `.venv/bin/pytest tests/ -q` shows zero failures.
2. Branch `v3-phase-6-multi-source-joins` is pushed to origin.
3. README has the Multi-Source Joins section.
4. Version is `0.3.0a5`.
5. CLI smoke test: with two CSVs and a config using `sources:`, run `siphon run --config siphon.yaml --create-tables` (no input path) and verify both tables are populated.

## Out of Scope

- **Right join, full outer join, cross join**: only left and inner are supported. Most ETL use cases need one of these two.
- **Joins on multiple keys (composite key)**: `on:` accepts a single field name. Compound keys would require a list. Defer until users ask.
- **Joins on different field names left vs right**: `on:` requires the same field name in both sources. Workaround: rename via mapper before joining (give one source a field with the same name as the other's key field).
- **Field name collisions**: when left and right both have a non-key field with the same name, left wins (documented in Joiner). The plan doesn't enforce uniqueness across sources; users are responsible for distinct names if collisions matter.
- **XML/JSON multi-source**: the loaders work with multi-source, but XML collections (nested arrays) are dropped in multi-source mode (collections require a single source's nested structure). Use single-source XML for that case.
- **CLI `--source name=path` overrides**: paths come from YAML only. Adding CLI overrides is a future enhancement.

## Self-Review Notes

**Spec coverage check:**
- ✅ `sources:` plural list with per-source name + path + fields — Task 2
- ✅ `joins:` list with left/right/on/type — Tasks 2, 4, 5
- ✅ Left and inner join semantics — Task 4
- ✅ Backward compat with `source:` singular — Task 2 (validator), Task 5 (pipeline branches)
- ✅ Pipeline orchestrates load → map per source → join → existing flow — Task 5
- ✅ CLI: input_path optional for multi-source — Task 6

**Type consistency:**
- `JoinConfig(left, right, on, type)` — same across Tasks 2, 4, 5.
- `join_records(left, right, *, on, join_type)` — same in Tasks 4, 5.
- `Pipeline._effective_fields()` and `Pipeline._build_source_mapper()` — defined and used in Task 5.

**Placeholder scan:** None.

**Risk areas flagged:**
1. Mutating `self._config.schema_.fields` in `run()` (Task 5) is a side effect on a shared config object. If the same `Pipeline` instance is run twice, the second call sees the already-flattened fields. This is fine functionally but worth noting. An alternative is to build the flat list lazily and pass it to component constructors instead of mutating config.
2. The `_build_source_mapper` uses `model_copy(deep=True)`. Pydantic's deep copy clones the entire config, which is mildly expensive for large configs but acceptable for the 1–10 sources case.
3. The chained-joins logic in Task 5 (`if j.left == first_left or j.left == "_merged"`) is a simplification. A user joining A→B then C→D as two independent joins would get `union(A⋈B, C⋈D)`. Multi-step chains where the second join references the first's output (e.g., `joins[1].left = "AB"`) are not supported in v3.6 and would require either a result-naming feature or an explicit graph executor. The README mentions "later joins extend it" only for the `joins[N].left == joins[0].left` case.
