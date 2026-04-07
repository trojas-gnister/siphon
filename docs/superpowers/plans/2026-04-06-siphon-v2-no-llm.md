# Siphon v2: Drop LLM, Add Sources + Transforms

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite Siphon to replace LLM-based extraction with explicit column mapping + a pluggable transform system, and add XML source support — enabling it to replace both the company uploader and the incident importer.

**Architecture:** Source loaders read data (CSV/XLSX/XML). A mapper applies explicit field mappings and transforms. Records flow through the existing validate → dedup → review → insert pipeline unchanged. Complex domain logic lives in Python transform files referenced from YAML config.

**Tech Stack:** Python 3.10+, asyncio, SQLAlchemy 2.0 (async), Pydantic 2.0, Pandas, xmltodict, Typer, Rich, PyYAML, pycountry, python-dateutil

**Spec:** This plan. The original v1 spec is at `docs/specs/2026-03-19-siphon-etl-pipeline-design.md`.

---

## What Changes from v1

| Component | v1 | v2 |
|-----------|----|----|
| Column mapping | LLM figures it out | Explicit `source:` in YAML |
| Input formats | CSV/XLSX/XLS/ODS | + XML, JSON |
| Transform system | Formatters only (14 types) | Formatters + built-in transforms + custom Python |
| LLM dependency | Required (openai) | Removed entirely |
| Nested collections | Not supported | XML/JSON → multi-table expansion |
| Constant fields | Not supported | `value:` in YAML |
| Computed fields | Not supported | `transform:` in YAML |
| ReviewBatch.revise() | LLM-powered | Removed (approve/reject only) |

## What Stays from v1

These modules are kept with no or minimal changes:
- `siphon/utils/formatters.py` — 14 pure formatting functions
- `siphon/utils/errors.py` — error hierarchy (minor additions)
- `siphon/utils/logger.py` — dual-logger setup
- `siphon/config/types.py` — field type registry
- `siphon/core/validator.py` — dynamic Pydantic validation + dedup
- `siphon/db/engine.py` — async SQLAlchemy engine
- `siphon/db/models.py` — dynamic ORM model generation
- `siphon/db/inserter.py` — relationship-aware insertion with topo sort

---

## File Structure

```
siphon/
├── pyproject.toml                   # Updated deps (remove openai, add xmltodict)
├── siphon/
│   ├── __init__.py                  # __version__ = "0.2.0"
│   ├── cli.py                       # Updated (remove LLM flags, update init template)
│   ├── config/
│   │   ├── __init__.py              # Updated re-exports
│   │   ├── loader.py                # Updated cross-validation for new config shape
│   │   ├── schema.py                # New SourceConfig, updated FieldConfig
│   │   └── types.py                 # Unchanged
│   ├── core/
│   │   ├── __init__.py
│   │   ├── pipeline.py              # Updated (source → map → validate → ... → insert)
│   │   ├── mapper.py                # NEW: applies field mappings + transforms to source data
│   │   ├── validator.py             # Unchanged
│   │   ├── reviewer.py              # Simplified (remove revise, remove LLMClient dep)
│   │   └── review_cli.py            # Updated (remove revision option)
│   ├── db/                          # Unchanged
│   │   ├── __init__.py
│   │   ├── engine.py
│   │   ├── models.py
│   │   └── inserter.py
│   ├── sources/                     # NEW: pluggable source loaders
│   │   ├── __init__.py
│   │   ├── base.py                  # SourceLoader protocol
│   │   ├── spreadsheet.py           # CSV/XLSX/XLS/ODS (extracted from old extractor.py)
│   │   └── xml.py                   # XML with nested collection expansion
│   ├── transforms/                  # NEW: transform system
│   │   ├── __init__.py
│   │   ├── builtins.py              # Built-in transforms (template, map, concat, uuid, etc.)
│   │   └── loader.py                # Load custom Python transform files
│   └── utils/
│       ├── __init__.py
│       ├── formatters.py            # Unchanged
│       ├── logger.py                # Unchanged
│       └── errors.py                # Add SourceError, TransformError; remove ExtractionError
└── tests/
    ├── conftest.py                  # Updated fixture (remove llm config)
    ├── fixtures/
    │   ├── sample_companies.csv     # Existing
    │   ├── example_config.yaml      # Updated (no llm section)
    │   ├── sample_incidents.xml     # NEW: Navex-style test data
    │   └── incident_config.yaml     # NEW: incident import config
    ├── sources/
    │   ├── test_spreadsheet.py
    │   └── test_xml.py
    ├── transforms/
    │   ├── test_builtins.py
    │   └── test_loader.py
    ├── config/
    │   ├── test_schema.py           # Updated
    │   └── test_loader.py           # Updated
    ├── core/
    │   ├── test_mapper.py           # NEW
    │   ├── test_validator.py        # Unchanged
    │   ├── test_reviewer.py         # Updated
    │   ├── test_pipeline.py         # Updated
    │   └── test_review_cli.py       # Updated
    ├── db/                          # Unchanged
    ├── utils/                       # Unchanged
    ├── test_cli.py                  # Updated
    └── test_integration.py          # Updated + new incident test
```

---

## v2 YAML Config Shape

### Company upload (replaces company_uploader)

```yaml
name: "company-import"

source:
  type: spreadsheet          # csv, xlsx, xls, ods
  # path provided via CLI argument

database:
  url: "${DATABASE_URL}"

schema:
  fields:
    - name: company_name
      source: "Company Name"       # column name or alias list
      aliases: ["Corp Name", "Business", "Entity Name"]
      type: string
      required: true
      min_length: 2
      db: { table: companies, column: name }

    - name: parent_entity
      source: "Parent Entity"
      aliases: ["Parent", "Parent Org", "Parent Company"]
      type: string
      db: { table: companies, column: parent_name }

    - name: phone
      source: "Phone"
      aliases: ["Phone Number", "Tel", "Telephone"]
      type: phone
      db: { table: companies, column: phone_number }

    - name: website
      source: "Website"
      aliases: ["URL", "Web", "Homepage"]
      type: url
      db: { table: companies, column: website_url }

    - name: address
      source: "Address"
      aliases: ["Street Address", "Location"]
      type: string
      min_length: 5
      db: { table: addresses, column: full_address }

    - name: state
      source: "State"
      aliases: ["State Code", "ST"]
      type: enum
      preset: us_states
      db: { table: addresses, column: state_code }

  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
    addresses:
      primary_key: { column: id, type: auto_increment }

  deduplication:
    key: [company_name]
    check_db: true
    match: case_insensitive

relationships:
  - type: belongs_to
    field: parent_entity
    table: companies
    references: companies
    fk_column: parent_id
    resolve_by: name
  - type: junction
    link: [companies, addresses]
    through: company_addresses
    columns:
      companies: company_id
      addresses: address_id

pipeline:
  chunk_size: 25
  review: true
  log_level: info
```

### Incident import (replaces incident importer)

```yaml
name: "navex-incident-import"

source:
  type: xml
  root: "Cases.Case"               # XPath-like to record list
  encoding: utf-8                   # or utf-16-le

transforms:
  file: ./incident_transforms.py    # Custom Python transform functions

variables:                           # Reusable values for templates
  reference_prefix: "MFRM"
  user_id: 928
  company_id: 437
  harassment_type_id: 14
  attachments_prefix: "MFRM_IMPORT"
  s3_url_prefix: "https://example-bucket.s3.amazonaws.com"

database:
  url: "mysql+aiomysql://${DB_USER}:${DB_PASS}@${DB_HOST}/${DB_NAME}"

schema:
  fields:
    # --- ws_incidents table ---
    - name: uuid
      transform: { type: uuid }
      db: { table: ws_incidents, column: uuid }

    - name: reference_number
      source: case_code
      transform: { type: template, template: "{reference_prefix}-{value}" }
      db: { table: ws_incidents, column: reference_number }

    - name: incident_status_id
      source: case_status
      transform:
        type: map
        values: { "Closed": 8, "In Process": 6, "Unreviewed": 3 }
        default: 8
      db: { table: ws_incidents, column: incident_status_id }

    - name: work_location
      transform:
        type: custom
        function: build_work_location
        args: [location_address, location_city, location_state,
               location_postal_code, location_country]
      db: { table: ws_incidents, column: work_location }

    - name: is_data_import
      value: true
      db: { table: ws_incidents, column: is_data_import }

    - name: company_id
      value: "${company_id}"       # from variables section
      type: integer
      db: { table: ws_incidents, column: company_id }

    - name: submitted_date
      source: date_reported
      type: datetime
      db: { table: ws_incidents, column: submitted_date }

    - name: date_added
      source: date_opened
      transform: { type: coalesce, fallback: { type: now } }
      db: { table: ws_incidents, column: date_added }
    # ... (60+ more fields, most with value: or source:)

  collections:
    # Nested arrays that expand into separate table rows
    - name: case_notes
      source_path: "CaseNotes.CaseNote"
      fields:
        - name: note
          source: case_note
          type: string
          db: { table: ws_incident_notes, column: note }
        - name: date_added
          source: date
          type: datetime
          db: { table: ws_incident_notes, column: date_added }
        - name: original_created_by
          source: user_name
          transform: { type: custom, function: reverse_name }
          db: { table: ws_incident_notes, column: original_created_by }
        - name: is_data_import
          value: true
          db: { table: ws_incident_notes, column: is_data_import }

    - name: attachments
      source_path: "Attachments.Attachment"
      fields:
        - name: file_name
          source: file_name
          db: { table: ws_file, column: name }
        - name: s3_key
          source: file_name
          transform:
            type: custom
            function: build_s3_key
            args: [case_code, file_name]
          db: { table: ws_file, column: s3_key }
        # ...

    - name: participants
      source_path: "Participants.Participant"
      fields:
        - name: first_name
          transform:
            type: custom
            function: resolve_first_name
            args: [first_name, last_name]
          db: { table: ws_incident_involved_party, column: first_name }
        # ...

  tables:
    ws_incidents:
      primary_key: { column: id, type: auto_increment }
    ws_incident_notes:
      primary_key: { column: id, type: auto_increment }
    ws_incident_meta:
      primary_key: { column: id, type: auto_increment }
    ws_file:
      primary_key: { column: id, type: auto_increment }
    ws_incident_file:
      primary_key: { column: id, type: auto_increment }
    ws_incident_involved_party:
      primary_key: { column: id, type: auto_increment }
    ws_incident_involved_party_role:
      primary_key: { column: id, type: auto_increment }
    ws_incident_recommendation:
      primary_key: { column: id, type: auto_increment }

relationships:
  - type: belongs_to
    field: reference_number
    table: ws_incident_notes
    references: ws_incidents
    fk_column: incident_id
    resolve_by: reference_number
  - type: belongs_to
    field: reference_number
    table: ws_incident_meta
    references: ws_incidents
    fk_column: parent_incident_id
    resolve_by: reference_number
  # ... etc for each FK relationship

pipeline:
  review: false
  log_level: info
```

---

## Execution Order (16 tasks)

| # | Task | Phase | Est. |
|---|------|-------|------|
| 1 | Update deps, errors, version | 0: Setup | 10m |
| 2 | SourceLoader protocol + spreadsheet loader | 1: Sources | 20m |
| 3 | XML source loader | 1: Sources | 30m |
| 4 | Built-in transform functions | 2: Transforms | 25m |
| 5 | Custom transform file loader | 2: Transforms | 20m |
| 6 | Update config schema (remove LLM, add source/transforms) | 3: Config | 30m |
| 7 | Update config loader + validation | 3: Config | 20m |
| 8 | Mapper: source data → target records | 4: Core | 30m |
| 9 | Collection expansion in mapper | 4: Core | 25m |
| 10 | Update pipeline (source → map → validate → insert) | 4: Core | 25m |
| 11 | Simplify reviewer (remove revise/LLM dep) | 4: Core | 15m |
| 12 | Update CLI + init template | 5: CLI | 20m |
| 13 | Update conftest + sample_config_dict fixture | 5: CLI | 10m |
| 14 | Integration test: company spreadsheet import | 6: Tests | 20m |
| 15 | Integration test: incident XML import | 6: Tests | 30m |
| 16 | Delete LLM module, final cleanup, README | 7: Polish | 15m |

---

## Task Details

### Task 1: Update Dependencies, Errors, Version

**Files:**
- Modify: `pyproject.toml`
- Modify: `siphon/__init__.py`
- Modify: `siphon/utils/errors.py`
- Modify: `tests/utils/test_errors.py`

- [ ] Add `xmltodict>=0.13.0` to dependencies in `pyproject.toml`. Remove `openai>=1.0.0`.

```toml
dependencies = [
    "sqlalchemy[asyncio]>=2.0.0",
    "pydantic>=2.0.0",
    "pandas>=2.0.0",
    "openpyxl>=3.0.0",
    "odfpy>=1.4",
    "typer>=0.9.0",
    "pyyaml>=6.0",
    "python-dotenv>=1.0.0",
    "rich>=13.0.0",
    "pycountry>=24.6.1",
    "python-dateutil>=2.8.0",
    "xmltodict>=0.13.0",
]
```

- [ ] Update version to `"0.2.0"` in `siphon/__init__.py`.

- [ ] In `siphon/utils/errors.py`, replace `ExtractionError` with `SourceError` and add `TransformError`:

```python
class SourceError(SiphonError):
    """Source loading failed — file not found, parse error, unsupported format."""
    pass

class TransformError(SiphonError):
    """Transform function failed."""
    pass
```

Keep `ExtractionError` as a deprecated alias for backward compat:
```python
ExtractionError = SourceError  # Deprecated: use SourceError
```

- [ ] Update `tests/utils/test_errors.py` to test `SourceError`, `TransformError`, and the `ExtractionError` alias.

- [ ] Run `pip install -e ".[dev]"` and `pytest tests/utils/test_errors.py`.

- [ ] Commit: `"feat: v2 deps — add xmltodict, remove openai, update error hierarchy"`

---

### Task 2: SourceLoader Protocol + Spreadsheet Loader

**Files:**
- Create: `siphon/sources/__init__.py`
- Create: `siphon/sources/base.py`
- Create: `siphon/sources/spreadsheet.py`
- Create: `tests/sources/__init__.py`
- Create: `tests/sources/test_spreadsheet.py`

- [ ] Write tests for spreadsheet loader:
  - CSV loads correctly (returns list[dict])
  - XLSX loads correctly
  - Column alias matching works (source column "Corp Name" matches field with alias)
  - Unsupported format raises SourceError
  - Missing file raises SourceError
  - Sheet parameter selects correct sheet

- [ ] Create `siphon/sources/base.py` with the `SourceLoader` protocol:

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class SourceLoader(Protocol):
    def load(self, path: str | Path, **kwargs) -> list[dict]:
        """Load source data and return a flat list of record dicts.
        
        Keys in each dict are the source column/field names.
        """
        ...
```

- [ ] Create `siphon/sources/spreadsheet.py` — extract the spreadsheet loading logic from the old `extractor.py`:

```python
class SpreadsheetLoader:
    def load(self, path: str | Path, *, sheet: str | int | None = None) -> list[dict]:
        """Load CSV/XLSX/XLS/ODS and return list of row dicts."""
        # Use pandas to read, fillna(""), convert to list of dicts
        # Return df.to_dict(orient="records")
```

- [ ] Tests pass.

- [ ] Commit: `"feat: SourceLoader protocol and spreadsheet loader"`

---

### Task 3: XML Source Loader

**Files:**
- Create: `siphon/sources/xml.py`
- Create: `tests/sources/test_xml.py`
- Create: `tests/fixtures/sample_incidents.xml`

- [ ] Create a small test XML file at `tests/fixtures/sample_incidents.xml`:

```xml
<?xml version="1.0" encoding="utf-8"?>
<Cases>
  <Case>
    <CaseCode>abc-123</CaseCode>
    <ReportNumber>RPT001</ReportNumber>
    <CaseStatus>Closed</CaseStatus>
    <OrgName>Acme Corp</OrgName>
    <LocationAddress>123 Main St</LocationAddress>
    <LocationCity>Springfield</LocationCity>
    <LocationState>IL</LocationState>
    <Details>Test incident details</Details>
    <DateOpened>2025-01-15 00:00:00.000</DateOpened>
    <CaseNotes>
      <CaseNote>
        <CaseNote>First note text</CaseNote>
        <Date>2025-01-16 10:00:00.000</Date>
        <UserName>Smith, John</UserName>
      </CaseNote>
    </CaseNotes>
    <Participants>
      <Participant>
        <FirstName>Jane</FirstName>
        <LastName>Doe</LastName>
        <Role>Reporter</Role>
      </Participant>
    </Participants>
    <Attachments>
      <Attachment>
        <FileName>report.pdf</FileName>
        <FileCode>FC001</FileCode>
        <DateUploaded>2025-01-17 00:00:00.000</DateUploaded>
        <Size>1024</Size>
      </Attachment>
    </Attachments>
  </Case>
</Cases>
```

- [ ] Write tests for XML loader:
  - Loads XML and returns list of dicts (one per Case)
  - Nested collections preserved as lists of dicts
  - Handles single-item collections (force_list behavior)
  - UTF-8 and UTF-16-LE encoding support
  - Handles duplicate root elements (takes first block)
  - Missing file raises SourceError
  - `root` parameter navigates to correct element path

- [ ] Implement `siphon/sources/xml.py`:

```python
import xmltodict
from siphon.sources.base import SourceLoader
from siphon.utils.errors import SourceError

class XMLLoader:
    def __init__(self, root: str, encoding: str = "utf-8",
                 force_list: list[str] | None = None):
        self._root = root
        self._encoding = encoding
        self._force_list = force_list or []

    def load(self, path: str | Path, **kwargs) -> list[dict]:
        """Load XML file, navigate to root path, return list of dicts."""
        # Read file with encoding
        # Handle duplicate root elements
        # Parse with xmltodict
        # Navigate root path (e.g., "Cases.Case")
        # Ensure result is a list
        # Return list[dict]
```

- [ ] Tests pass.

- [ ] Commit: `"feat: XML source loader with encoding and collection support"`

---

### Task 4: Built-in Transform Functions

**Files:**
- Create: `siphon/transforms/__init__.py`
- Create: `siphon/transforms/builtins.py`
- Create: `tests/transforms/__init__.py`
- Create: `tests/transforms/test_builtins.py`

- [ ] Write tests for each built-in transform:
  - `template`: `"{prefix}-{value}"` with context dict → formatted string
  - `map`: lookup dict + default → mapped value
  - `concat`: list of field values + separator → joined string (skipping None/empty)
  - `uuid`: → valid UUID4 string
  - `now`: → current UTC datetime string
  - `coalesce`: list of values → first non-null
  - `constant`: returns fixed value (this is handled in mapper, not here)

- [ ] Implement `siphon/transforms/builtins.py`:

```python
import uuid as _uuid
from datetime import datetime, timezone

def transform_template(value: str | None, *, template: str,
                       context: dict) -> str:
    """Apply a template string using context variables.
    
    {value} is replaced with the source field value.
    Other {keys} are replaced from the context dict.
    """
    return template.format(value=value or "", **context)

def transform_map(value: str | None, *, values: dict,
                  default=None) -> any:
    """Map a value through a lookup dict."""
    if value is None:
        return default
    return values.get(str(value), default)

def transform_concat(*, fields: list, separator: str = " ") -> str:
    """Concatenate non-empty values with a separator."""
    parts = [str(v) for v in fields if v is not None and str(v).strip()]
    return separator.join(parts) if parts else ""

def transform_uuid() -> str:
    """Generate a UUID4 string."""
    return str(_uuid.uuid4())

def transform_now(*, format: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc).strftime(format)

def transform_coalesce(*, fields: list, fallback=None) -> any:
    """Return the first non-null value from fields."""
    for v in fields:
        if v is not None and str(v).strip():
            return v
    return fallback

BUILTIN_TRANSFORMS = {
    "template": transform_template,
    "map": transform_map,
    "concat": transform_concat,
    "uuid": transform_uuid,
    "now": transform_now,
    "coalesce": transform_coalesce,
}
```

- [ ] Tests pass.

- [ ] Commit: `"feat: built-in transform functions (template, map, concat, uuid, now, coalesce)"`

---

### Task 5: Custom Transform File Loader

**Files:**
- Create: `siphon/transforms/loader.py`
- Create: `tests/transforms/test_loader.py`

- [ ] Write tests:
  - Loads a Python file and extracts callable functions by name
  - Missing file raises ConfigError
  - Function not found raises TransformError
  - Non-callable attribute raises TransformError
  - None/empty path returns empty registry

- [ ] Implement `siphon/transforms/loader.py`:

```python
import importlib.util
from pathlib import Path
from siphon.utils.errors import ConfigError, TransformError

def load_custom_transforms(path: str | Path | None) -> dict[str, callable]:
    """Load a Python file and return a dict of {name: function}.
    
    Only includes public callables (no underscore prefix).
    """
    if path is None:
        return {}
    
    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Transform file not found: {path}")
    
    spec = importlib.util.spec_from_file_location("custom_transforms", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    transforms = {}
    for name in dir(module):
        if not name.startswith("_"):
            obj = getattr(module, name)
            if callable(obj):
                transforms[name] = obj
    
    return transforms
```

- [ ] Tests pass.

- [ ] Commit: `"feat: custom Python transform file loader"`

---

### Task 6: Update Config Schema

**Files:**
- Modify: `siphon/config/schema.py`
- Modify: `tests/config/test_schema.py`

This is the largest schema change. Remove `LLMConfig`. Add `SourceConfig`, `TransformConfig`, `VariablesConfig`, `CollectionConfig`. Update `FieldConfig` with `source`, `aliases`, `transform`, `value`.

- [ ] Write tests for new config models:
  - `SourceConfig` with type, root, encoding, force_list
  - `FieldConfig` with `source`, `aliases`, `transform`, `value`
  - Field must have exactly one of: `source`, `transform` (with no source), or `value`
  - `TransformFieldConfig` with type, template, values, default, function, args, etc.
  - `CollectionConfig` with source_path and fields
  - `SiphonConfig` no longer requires `llm`
  - Cross-validation still works for table references

- [ ] Update `siphon/config/schema.py`:

Replace `LLMConfig` with:
```python
class SourceConfig(BaseModel):
    type: Literal["spreadsheet", "xml", "json"]
    root: str | None = None            # For XML/JSON: path to record list
    encoding: str = "utf-8"            # For XML: file encoding
    force_list: list[str] | None = None  # For XML: elements to force as lists

class TransformFieldConfig(BaseModel):
    """Inline transform definition on a field."""
    type: str                          # template, map, concat, uuid, now, coalesce, custom
    template: str | None = None        # For template type
    values: dict | None = None         # For map type
    default: Any | None = None         # For map type
    fields: list[str] | None = None    # For concat/coalesce type
    separator: str = " "               # For concat type
    function: str | None = None        # For custom type
    args: list[str] | None = None      # For custom type: source field names to pass
    format: str | None = None          # For now type
    fallback: "TransformFieldConfig | None" = None  # For coalesce type

class TransformFileConfig(BaseModel):
    file: str | None = None            # Path to custom Python transforms
```

Update `FieldConfig`:
```python
class FieldConfig(BaseModel):
    name: str
    type: FieldType | None = None      # Optional now — not all fields need formatting
    source: str | None = None          # Source column name
    aliases: list[str] | None = None   # Alternative column names
    transform: TransformFieldConfig | None = None  # Inline transform
    value: Any | None = None           # Constant value
    db: FieldDBConfig
    required: bool = False
    # ... (keep existing constraints)
```

Update `SiphonConfig`:
```python
class SiphonConfig(BaseModel):
    name: str
    source: SourceConfig
    database: DatabaseConfig
    schema_: SchemaConfig = Field(alias="schema")
    transforms: TransformFileConfig | None = None
    variables: dict[str, Any] | None = None
    relationships: list[Relationship] = []
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
```

Add to `SchemaConfig`:
```python
class CollectionConfig(BaseModel):
    name: str
    source_path: str               # Path within source record (e.g., "CaseNotes.CaseNote")
    fields: list[FieldConfig]

class SchemaConfig(BaseModel):
    fields: list[FieldConfig]
    collections: list[CollectionConfig] | None = None
    tables: dict[str, TableConfig]
    deduplication: DeduplicationConfig | None = None
```

- [ ] Update tests.

- [ ] Tests pass.

- [ ] Commit: `"feat: v2 config schema — source, transforms, collections, remove LLM"`

---

### Task 7: Update Config Loader + Validation

**Files:**
- Modify: `siphon/config/loader.py`
- Modify: `tests/config/test_loader.py`

- [ ] Write tests:
  - Valid v2 config loads (spreadsheet source, no llm)
  - Valid XML source config loads
  - Variables section substituted into templates at load time
  - Custom transform file path validated
  - Cross-validation: custom transform `function` exists in transform file
  - Cross-validation: collection source_path fields reference valid tables

- [ ] Update `load_config()`:
  - Remove LLM-specific cross-validation (enum/regex/subdivision checks stay)
  - Add validation: if field has `transform.type == "custom"`, a `transforms.file` must be configured
  - Add variable substitution: `${var_name}` in `value` fields resolved from `variables` section
  - Add validation: field must have at least one of `source`, `transform`, or `value`

- [ ] Tests pass.

- [ ] Commit: `"feat: v2 config loader with transform and variable validation"`

---

### Task 8: Mapper — Source Data to Target Records

**Files:**
- Create: `siphon/core/mapper.py`
- Create: `tests/core/test_mapper.py`

- [ ] Write tests:
  - Direct source mapping: field with `source: "Company Name"` extracts value
  - Alias matching: source column "Corp Name" matches field with alias
  - Constant value: field with `value: true` always returns true
  - Built-in transform: `template`, `map`, `concat` applied correctly
  - Custom transform: function called with correct args
  - UUID/now transforms generate values
  - Coalesce transform picks first non-null
  - Missing source column returns None (unless required)
  - Full record mapping: multiple fields mapped from one source record

- [ ] Implement `siphon/core/mapper.py`:

```python
class Mapper:
    def __init__(self, config: SiphonConfig, custom_transforms: dict[str, callable] | None = None):
        self._config = config
        self._custom = custom_transforms or {}
        self._variables = config.variables or {}
        self._alias_map = self._build_alias_map()
    
    def _build_alias_map(self) -> dict[str, str]:
        """Build {lowercase_alias: field_name} from all fields."""
        alias_map = {}
        for field in self._config.schema_.fields:
            if field.source:
                alias_map[field.source.lower()] = field.source
            for alias in (field.aliases or []):
                alias_map[alias.lower()] = field.source
        return alias_map
    
    def _resolve_column(self, record: dict, source: str) -> any:
        """Find a value in a record, trying exact match then aliases."""
        if source in record:
            return record[source]
        source_lower = source.lower()
        for key in record:
            if key.lower() == source_lower:
                return record[key]
        return None
    
    def _apply_transform(self, transform_config, value, record) -> any:
        """Apply a transform to a value."""
        # Dispatch to built-in or custom transform
    
    def map_record(self, source_record: dict) -> dict:
        """Map a single source record to target field names."""
        result = {}
        for field in self._config.schema_.fields:
            if field.value is not None:
                result[field.name] = field.value
            elif field.transform and not field.source:
                result[field.name] = self._apply_transform(
                    field.transform, None, source_record
                )
            elif field.source:
                value = self._resolve_column(source_record, field.source)
                if field.transform:
                    value = self._apply_transform(
                        field.transform, value, source_record
                    )
                result[field.name] = value
        return result
    
    def map_records(self, source_records: list[dict]) -> list[dict]:
        """Map all source records."""
        return [self.map_record(r) for r in source_records]
```

- [ ] Tests pass.

- [ ] Commit: `"feat: mapper — applies field mappings and transforms to source data"`

---

### Task 9: Collection Expansion in Mapper

**Files:**
- Modify: `siphon/core/mapper.py`
- Modify: `tests/core/test_mapper.py`

- [ ] Write tests:
  - Source record with nested list `case_notes: [{note: "A"}, {note: "B"}]` produces 2 mapped records for the collection's table
  - Collection fields are mapped from nested item, not parent
  - Parent fields can be referenced via transform args
  - Empty nested list produces no records
  - Collection records include parent's mapped values for FK resolution

- [ ] Add `map_collections()` to Mapper:

```python
def map_collections(self, source_record: dict, 
                    parent_mapped: dict) -> dict[str, list[dict]]:
    """Expand nested collections from a source record.
    
    Returns {collection_name: [mapped_records]}.
    Each mapped record includes fields from the collection config
    plus any parent fields needed for FK resolution.
    """
    if not self._config.schema_.collections:
        return {}
    
    result = {}
    for collection in self._config.schema_.collections:
        items = self._navigate_path(source_record, collection.source_path)
        if not items:
            continue
        if not isinstance(items, list):
            items = [items]
        
        mapped_items = []
        for item in items:
            # Merge parent record context for transforms that reference parent fields
            context = {**source_record, **item}
            mapped = {}
            for field in collection.fields:
                if field.value is not None:
                    mapped[field.name] = field.value
                elif field.transform and not field.source:
                    mapped[field.name] = self._apply_transform(
                        field.transform, None, context
                    )
                elif field.source:
                    value = item.get(field.source)
                    if field.transform:
                        value = self._apply_transform(
                            field.transform, value, context
                        )
                    mapped[field.name] = value
            mapped_items.append(mapped)
        
        result[collection.name] = mapped_items
    
    return result

def _navigate_path(self, data: dict, path: str) -> any:
    """Navigate a dot-separated path in nested dict."""
    parts = path.split(".")
    current = data
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
        if current is None:
            return None
    return current
```

- [ ] Tests pass.

- [ ] Commit: `"feat: collection expansion — nested arrays to multi-table records"`

---

### Task 10: Update Pipeline

**Files:**
- Modify: `siphon/core/pipeline.py`
- Modify: `tests/core/test_pipeline.py`

- [ ] Write tests:
  - Pipeline loads source via appropriate loader
  - Pipeline maps records via Mapper
  - Pipeline validates mapped records
  - Dry run still works
  - Collection records are validated and inserted alongside main records
  - Pipeline result counts are accurate

- [ ] Rewrite `Pipeline.run()`:

```python
async def run(self, input_path, *, dry_run=False, no_review=False,
              create_tables=False, sheet=None):
    # 1. Setup logging
    # 2. Load source data via appropriate loader
    source_config = self._config.source
    if source_config.type == "spreadsheet":
        loader = SpreadsheetLoader()
        source_records = loader.load(input_path, sheet=sheet)
    elif source_config.type == "xml":
        loader = XMLLoader(root=source_config.root, ...)
        source_records = loader.load(input_path)
    
    # 3. Load custom transforms if configured
    custom_transforms = load_custom_transforms(
        self._config.transforms.file if self._config.transforms else None
    )
    
    # 4. Map source records to target schema
    mapper = Mapper(self._config, custom_transforms)
    records = mapper.map_records(source_records)
    
    # 5. Map collections (if any)
    all_collection_records = defaultdict(list)
    for source_rec, mapped_rec in zip(source_records, records):
        collections = mapper.map_collections(source_rec, mapped_rec)
        for name, items in collections.items():
            all_collection_records[name].extend(items)
    
    # 6. Validate all records (main + collections)
    # 7. Deduplicate
    # 8. Review (if enabled)
    # 9. Insert (main records + collection records)
```

- [ ] Update existing pipeline tests (remove LLM mocking, use source loading).

- [ ] Tests pass.

- [ ] Commit: `"feat: v2 pipeline — source loading + mapping replaces LLM extraction"`

---

### Task 11: Simplify Reviewer

**Files:**
- Modify: `siphon/core/reviewer.py`
- Modify: `siphon/core/review_cli.py`
- Modify: `tests/core/test_reviewer.py`
- Modify: `tests/core/test_review_cli.py`

- [ ] Remove `revise()` method from `ReviewBatch`. Remove `LLMClient` from constructor. Remove `revision_count` property.

```python
class ReviewBatch:
    def __init__(self, records: list[dict], config: SiphonConfig,
                 inserter: Inserter | None = None):
        self._records = records
        self._config = config
        self._inserter = inserter
        self._status = ReviewStatus.PENDING
    
    def approve(self) -> None: ...
    def reject(self) -> None: ...
    def get_summary(self) -> dict: ...
    def get_sql_preview(self) -> list[str]: ...
```

- [ ] Update `ReviewCLI.run_review()`: remove "or type a revision command" option. Only approve/reject.

- [ ] Update tests — remove revision-related tests, remove LLMClient mocking.

- [ ] Tests pass.

- [ ] Commit: `"refactor: simplify reviewer — remove LLM revision, approve/reject only"`

---

### Task 12: Update CLI + Init Template

**Files:**
- Modify: `siphon/cli.py`
- Modify: `tests/test_cli.py`

- [ ] Remove `--chunk-size` flag (no longer relevant without LLM chunking — or keep if useful for batch insert sizing).

- [ ] Update `INIT_TEMPLATE` to reflect v2 config shape:
  - Replace `llm:` section with `source:` section
  - Add `variables:` section example
  - Add `transforms:` section example
  - Add `collections:` example (commented out)
  - Keep field examples but add `source:` and `aliases:` examples

- [ ] Update `run` command — remove LLM-related logic.

- [ ] Update CLI tests.

- [ ] Tests pass.

- [ ] Commit: `"feat: v2 CLI with updated init template"`

---

### Task 13: Update Conftest + sample_config_dict Fixture

**Files:**
- Modify: `tests/conftest.py`
- Modify: `tests/fixtures/example_config.yaml`

- [ ] Update `sample_config_dict` to v2 shape (replace `llm` with `source`):

```python
@pytest.fixture
def sample_config_dict():
    return {
        "name": "test_pipeline",
        "source": {
            "type": "spreadsheet",
        },
        "database": {
            "url": "sqlite+aiosqlite:///test.db",
        },
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "source": "Company Name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
    }
```

- [ ] Update `tests/fixtures/example_config.yaml` to v2 shape.

- [ ] Run full test suite. Fix any tests that fail due to config shape changes.

- [ ] Commit: `"fix: update test fixtures for v2 config shape"`

---

### Task 14: Integration Test — Company Spreadsheet Import

**Files:**
- Modify: `tests/test_integration.py`

- [ ] Rewrite the company import integration test to use v2 pipeline (no LLM mocking):
  - Create a CSV with known column names matching the config's `source:` fields
  - Run the pipeline with `create_tables=True`, `no_review=True`
  - Verify companies table, addresses table, parent FK, junction rows, dedup

- [ ] Tests pass.

- [ ] Commit: `"test: v2 integration test for company spreadsheet import"`

---

### Task 15: Integration Test — Incident XML Import

**Files:**
- Create: `tests/fixtures/incident_config.yaml`
- Create: `tests/fixtures/incident_transforms.py`
- Modify: `tests/test_integration.py`

- [ ] Create `tests/fixtures/incident_transforms.py` with the custom transform functions needed:

```python
def build_work_location(address, city, state, postal_code, country):
    parts = [p for p in [address, city, state, postal_code, country] if p]
    return " ".join(str(p) for p in parts).replace('"', '')

def reverse_name(name):
    if not name or "," not in name:
        return name
    parts = name.split(",", 1)
    return f"{parts[1].strip()} {parts[0].strip()}"

def resolve_first_name(first_name, last_name):
    if first_name and first_name.strip():
        return first_name.strip()
    if last_name and last_name.strip():
        return last_name.strip()
    return "Unknown"
```

- [ ] Create `tests/fixtures/incident_config.yaml` with XML source config, transforms, variables, and collection mappings for at least 2-3 tables.

- [ ] Write integration test:
  - Uses `sample_incidents.xml` from Task 3
  - Full pipeline with `create_tables=True`, `no_review=True`
  - Verify incidents table has correct fields (reference_number, work_location, status)
  - Verify incident_notes table has expanded case notes
  - Verify FK resolution between notes and incidents
  - Verify custom transforms were applied (reverse_name, build_work_location)

- [ ] Tests pass.

- [ ] Commit: `"test: v2 integration test for incident XML import"`

---

### Task 16: Delete LLM Module, Final Cleanup, README

**Files:**
- Delete: `siphon/llm/` (entire directory)
- Delete: `tests/llm/` (entire directory)
- Modify: `siphon/__init__.py` (update re-exports)
- Modify: `siphon/core/__init__.py` (update re-exports)
- Modify: `README.md`

- [ ] Delete `siphon/llm/client.py`, `siphon/llm/prompts.py`, `siphon/llm/__init__.py`.
- [ ] Delete `tests/llm/test_client.py`, `tests/llm/test_prompts.py`, `tests/llm/__init__.py`.
- [ ] Delete old `siphon/core/extractor.py` and `tests/core/test_extractor.py` (replaced by sources + mapper).

- [ ] Update `siphon/__init__.py` re-exports (remove LLM imports).

- [ ] Update `siphon/core/__init__.py` (replace Extractor with Mapper).

- [ ] Update `README.md`:
  - Remove LLM section
  - Add source types section (spreadsheet, XML)
  - Add transforms section (built-in + custom)
  - Add collections section
  - Update example configs
  - Update CLI reference

- [ ] Run full test suite with coverage: `pytest tests/ -v --cov=siphon`

- [ ] Commit: `"chore: v2 cleanup — remove LLM module, update README"`

---

## Verification

After all tasks:

1. **Unit tests:** `pytest tests/ -v --cov=siphon` — all pass
2. **CLI smoke test:** `siphon --help`, `siphon init`, `siphon validate --config siphon.yaml`
3. **Company import test:** Task 14 verifies full pipeline with spreadsheet source
4. **Incident import test:** Task 15 verifies full pipeline with XML source + transforms + collections
5. **No openai dependency:** `pip show openai` returns "not found"

## Key Design Decisions

1. **LLM removed entirely** — not optional, not a mode. Column mapping is explicit via `source:` + `aliases:`.

2. **Transform system is layered** — built-in transforms (template, map, concat, uuid, now, coalesce) cover 80% of cases. Custom Python file covers the rest. No need for a plugin architecture.

3. **Collections are first-class** — nested arrays in XML/JSON are declared in config and automatically expanded into separate table rows with FK resolution.

4. **Variables section** — reusable config values (reference_prefix, user_id, etc.) that can be referenced in templates and constant values. Avoids repetition.

5. **ReviewBatch simplified** — approve/reject only. No LLM revision. If data is wrong, fix the source or transform.

6. **Backward-incompatible** — v2 config is not compatible with v1. This is intentional. The v1 config shape with `llm:` section is gone.
