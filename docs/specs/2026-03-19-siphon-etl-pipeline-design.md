# Siphon — Configurable LLM-Powered ETL Pipeline

**Date:** 2026-03-19
**Status:** Approved
**Replaces:** Legacy single-purpose `company_uploader`

## Overview

Siphon is an open-source, configurable ETL pipeline that uses LLMs to intelligently map messy spreadsheet data to a user-defined schema, validates it, runs a human-in-the-loop review, and inserts it into any SQL database. It is driven entirely by a single YAML config file — no Python required from users.

## Core Value Proposition

End-to-end pipeline: **ingest → AI mapping → validate → HITL review → insert**, all configurable via YAML.

## Decisions Log

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Schema definition | YAML config only (v1) | Current use case has no custom validators; built-in types cover all needs |
| Database support | Any SQL via SQLAlchemy | Already a dependency; connection string swap is all that's needed |
| DB mapping | Inline in YAML config | Single file keeps everything in one place |
| Relationships | `belongs_to` + `junction` | Covers self-referential hierarchies and many-to-many; no full ORM modeling |
| LLM provider | `openai` library with configurable `base_url` | Works with Ollama, vLLM, LM Studio, OpenAI — zero extra dependencies |
| LLM framework | Raw `openai` library (no LangChain/LlamaIndex) | Interaction is simple prompt→JSON; frameworks add weight without proportional value |
| Ingestion modes | Spreadsheet only (dropped interactive Q&A) | Q&A was legacy-specific UX; spreadsheet mode handles bulk data |
| HITL review | CLI v1, designed for future web UI | Clean API layer separates review logic from presentation |
| Extraction prompt | Auto-generated from schema + optional `extraction_hints` | Covers 90% of cases; hints handle domain-specific quirks |
| Table creation | Flag-controlled (`--create-tables`) | Safe default (require existing tables), opt-in convenience |
| Language | Python 3.10+ | Bottleneck is I/O not computation; best ecosystem for ETL/LLM/data |
| CLI framework | Typer | Minimal code for `--help`, subcommands, argument parsing |
| Async model | Async throughout (asyncio) | Pipeline is I/O-bound (LLM calls, DB); async enables concurrent chunk processing |
| Future extensibility | Evolve toward Python escape hatches (v2) | YAGNI — ship pure YAML first, add custom transforms if community asks |

## User Experience

### Installation & Usage

```bash
pip install siphon-etl
siphon init                          # generate starter config
siphon run --config siphon.yaml data/customers.xlsx
```

### Supported Input Formats

`.xlsx`, `.xls`, `.csv`, `.ods` — detected by file extension. When given a directory path, Siphon processes all supported files in that directory.

For multi-sheet Excel files, Siphon reads only the first sheet by default. A `--sheet` flag can specify a sheet name or index.

### CLI Commands

| Command | Purpose |
|---------|---------|
| `siphon run` | Execute the full pipeline |
| `siphon validate` | Check config for errors without running |
| `siphon init` | Generate a starter YAML config (static template with commented examples for every option) |

### CLI Flags

| Flag | Purpose |
|------|---------|
| `--config` | Path to YAML config (default: `./siphon.yaml`) |
| `--create-tables` | Auto-create tables if they don't exist |
| `--dry-run` | Extract + validate only, no DB insertion |
| `--no-review` | Skip HITL review, insert directly |
| `--chunk-size N` | Override chunk size |
| `--sheet NAME` | Specify sheet name or index for multi-sheet Excel files |
| `--verbose` / `--quiet` | Log level override |

## YAML Config Structure

```yaml
name: "company-import"

llm:
  base_url: "http://localhost:11434/v1"
  model: "llama3"
  api_key: ""                        # optional, for remote providers
  extraction_hints: |                # optional, domain-specific guidance
    If a row has no company name but has a city,
    use the parent name + city as the company name.

database:
  url: "${DATABASE_URL}"             # env var substitution supported

schema:
  fields:
    - name: company_name
      type: string
      required: true
      min_length: 2
      db:
        table: companies
        column: name
    - name: parent_entity
      type: string
      db:
        table: companies
        column: parent_name
    - name: phone
      type: phone
      db:
        table: companies
        column: phone_number
    - name: website
      type: url
      db:
        table: companies
        column: website_url
    - name: address
      type: string
      min_length: 5
      db:
        table: addresses
        column: full_address
    - name: state
      type: enum
      preset: us_states
      db:
        table: addresses
        column: state_code

  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
    addresses:
      primary_key: { column: id, type: auto_increment }

  deduplication:
    key: [company_name]
    check_db: true
    match: case_insensitive            # exact | case_insensitive

relationships:
  - type: belongs_to
    field: parent_entity
    table: companies
    references: companies
    fk_column: parent_id               # auto-generated FK column name
    resolve_by: name                   # match parent_entity value against 'name' column
  - type: junction
    link: [companies, addresses]
    through: company_addresses
    columns:                           # junction table FK column names
      companies: company_id
      addresses: address_id

pipeline:
  chunk_size: 25
  review: true
  log_level: info                      # debug | info | warning | error
  log_dir: ./logs
```

### Environment Variable Substitution

The `${ENV_VAR}` syntax is supported in all string values throughout the YAML config. Siphon loads `.env` files via `python-dotenv` if present. If a referenced env var is not set, Siphon raises a `ConfigError` at startup with the variable name and the config path where it was referenced.

### Extraction Hints Behavior

`extraction_hints` are appended to the auto-generated extraction prompt as supplementary instructions. They cannot override the schema structure (the field list and output format are always controlled by the schema definition). Hints that contradict the schema (e.g., "return XML instead of JSON") are ignored by the prompt structure. Hints are intended for domain-specific mapping guidance — e.g., how to handle missing values, which columns to prefer, naming conventions.

## Architecture

### Project Structure

```
siphon/
├── pyproject.toml
├── siphon/
│   ├── __init__.py
│   ├── cli.py                  # Typer CLI entry point
│   ├── config/
│   │   ├── loader.py           # YAML parsing + env var substitution
│   │   ├── schema.py           # Pydantic models for config validation
│   │   └── types.py            # Built-in field type registry
│   ├── core/
│   │   ├── pipeline.py         # Orchestrator: load → extract → validate → review → insert
│   │   ├── extractor.py        # LLM-powered schema mapping (chunked)
│   │   ├── validator.py        # Dynamic Pydantic model generation + validation
│   │   └── reviewer.py         # HITL review engine (approve/reject/revise)
│   ├── db/
│   │   ├── engine.py           # SQLAlchemy async engine + session management
│   │   ├── models.py           # Dynamic ORM model generation from config
│   │   └── inserter.py         # Relationship-aware insertion logic
│   ├── llm/
│   │   ├── client.py           # Async OpenAI-compatible client wrapper
│   │   └── prompts.py          # Prompt templates (extraction + revision)
│   └── utils/
│       ├── formatters.py       # Built-in type formatters (phone, url, etc.)
│       ├── logger.py           # Logging setup
│       └── errors.py           # Custom exception hierarchy
├── presets/
│   └── us_states.yaml
└── tests/
```

### Async Model

The entire pipeline is async (`asyncio`). This is the same model as the existing codebase and is the right fit for an I/O-bound pipeline:

- **LLM client** — `AsyncOpenAI` for non-blocking LLM calls. Chunks can be processed concurrently via `asyncio.gather()`.
- **Database** — SQLAlchemy async sessions (`AsyncSession`) for non-blocking DB operations.
- **Pipeline orchestrator** — `async def run()` coordinates the flow.
- **CLI** — Typer invokes the async pipeline via `asyncio.run()`.

### Data Flow

```
YAML Config
    ↓
┌─────────────┐
│  cli.py     │  Parse args, load config
└──────┬──────┘
       ↓
┌─────────────┐
│ pipeline.py │  Orchestrates the full flow
└──────┬──────┘
       ↓
┌─────────────┐
│ extractor   │  Load spreadsheet → chunk → LLM maps to schema → JSON
└──────┬──────┘
       ↓
┌─────────────┐
│ validator   │  Build Pydantic model from config → validate each record
└──────┬──────┘
       ↓
┌─────────────┐
│ reviewer    │  Display records → approve / reject / revise (→ back to LLM)
└──────┬──────┘
       ↓
┌─────────────┐
│ inserter    │  Generate ORM models → resolve relationships → insert
└──────┬──────┘
       ↓
    Database
```

## Built-in Field Types

| Type | Validation | Formatting | SQL Type (`--create-tables`) | Example |
|------|-----------|------------|------------------------------|---------|
| `string` | `min_length`, `max_length`, `required` | Strip whitespace | `VARCHAR(255)` | `"  Acme "` → `"Acme"` |
| `integer` | `min`, `max` | Cast to int | `INTEGER` | `"42"` → `42` |
| `number` | `min`, `max` | Cast to float | `FLOAT` | `"42.5"` → `42.5` |
| `currency` | Numeric after stripping symbols | Strip `$`, `,`, handle negatives | `DECIMAL(12,2)` | `"$1,234.56"` → `1234.56` |
| `phone` | 10-11 digits | US format | `VARCHAR(20)` | `"5551234567"` → `"(555) 123-4567"` |
| `url` | Basic URL structure | Prepend `http://` if missing | `VARCHAR(500)` | `"acme.com"` → `"http://acme.com"` |
| `email` | RFC-basic validation | Lowercase | `VARCHAR(255)` | `"Bob@Acme.COM"` → `"bob@acme.com"` |
| `date` | Parseable date string | Output `format` (default: `%Y-%m-%d`) | `DATE` | `"3/19/2026"` → `"2026-03-19"` |
| `datetime` | Parseable datetime string | Output `format` (default: `%Y-%m-%dT%H:%M:%S`) | `DATETIME` | `"3/19/2026 2:30 PM"` → `"2026-03-19T14:30:00"` |
| `enum` | Value in list or preset | Configurable: `case: upper\|lower\|preserve` (default: `upper`) | `VARCHAR(50)` | `"ca"` → `"CA"` |
| `boolean` | Truthy/falsy detection | Cast to bool | `BOOLEAN` | `"yes"` → `true` |
| `regex` | Matches user-defined pattern (pass/fail validation only, no capture group extraction) | No formatting | `VARCHAR(255)` | `"ABC-1234"` validated against `^[A-Z]{3}-\d{4}$` |

### Date/Datetime Format Configuration

```yaml
- name: signup_date
  type: date
  format: "%Y-%m-%d"        # output format, default if omitted

- name: last_login
  type: datetime
  format: "%Y-%m-%dT%H:%M:%S"  # output format, default if omitted
```

**Enum presets** ship as YAML files in `presets/` (e.g., `us_states`). Users can add custom preset files.

## LLM Extraction

### Prompt Generation

The extraction prompt is auto-generated from `schema.fields`:

```
You are a data extraction assistant. Given CSV data, extract the
following fields for EACH row:

Fields to extract:
- company_name (string, required)
- parent_entity (string)
- phone (phone number)
- website (url)
- address (string)
- state (enum: AL, AK, AZ, ... WY, DC)

Additional instructions:
{extraction_hints from config}

Rules:
- Return a JSON array with exactly {N} objects, one per input row
- Every object must have all fields listed above
- Use empty string "" for missing values
- Do not skip or duplicate rows

CSV data:
{chunk_csv}
```

### Row Count Mismatch Handling

After parsing the LLM response, Siphon checks that the number of returned objects matches the number of input rows. On mismatch:

1. **First attempt:** Retry the chunk once with an explicit correction prompt: "You returned {M} objects but the input had {N} rows. Return exactly {N} objects."
2. **Second failure:** Log a warning with the chunk range, skip the chunk, continue with remaining chunks. Report skipped chunks in the final summary.

### Revision Prompt

When a user gives a natural language revision command during HITL review:

```
Here is a batch of extracted data as JSON:
{current_batch_json}

Apply this modification:
"{user_command}"

Return the modified data as a JSON array with the same structure.
```

Revised JSON is re-validated before returning to review.

### Response Parsing

- Extract JSON from response (handle markdown code fences, extra text)
- Unwrap nested objects if the LLM wraps the array in a parent key
- Return list of dicts

## Dynamic ORM & Database Insertion

### Primary Keys

Each table declared in `schema.tables` must specify a primary key:

```yaml
schema:
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
    addresses:
      primary_key: { column: id, type: auto_increment }
```

Supported primary key types:
- `auto_increment` — `Integer` with `autoincrement=True` (default)
- `uuid` — `String(36)`, auto-generated UUID4 by Siphon before insert

The primary key column is not a schema extraction field — it is managed by Siphon/the database. After insertion, the generated ID is captured and used for relationship resolution.

### Model Generation

From `schema.fields[].db` mappings, Siphon groups fields by table and generates SQLAlchemy ORM classes dynamically at runtime using `type()` + `DeclarativeBase`. Each table gets:

- The primary key column (from `schema.tables` config)
- All mapped field columns (with SQL types derived from the built-in field type — see Built-in Field Types table)
- Any auto-generated FK columns (from `belongs_to` relationships)

### Relationship Handling

#### `belongs_to`

```yaml
relationships:
  - type: belongs_to
    field: parent_entity           # schema field containing the reference value
    table: companies               # table that gets the FK column
    references: companies          # target table (self-referential in this case)
    fk_column: parent_id           # FK column added to the ORM model (Integer, nullable)
    resolve_by: name               # column in target table to match field value against
```

**Resolution process:**
1. During insertion, Siphon builds a lookup cache: `{target_table.resolve_by_value → target_table.id}`
2. Pre-populated from existing DB rows (if `deduplication.check_db: true`)
3. Updated as new rows are inserted in the current batch
4. For self-referential relationships, Siphon topologically sorts records so parents are inserted before children
5. The string value from `field` is resolved to an integer ID via the cache, stored in `fk_column`

#### `junction`

```yaml
relationships:
  - type: junction
    link: [companies, addresses]
    through: company_addresses     # junction table name
    columns:                       # FK column names in the junction table
      companies: company_id        # FK to companies.id
      addresses: address_id        # FK to addresses.id
```

**Behavior:** After inserting a record that spans both linked tables, Siphon inserts a row into the junction table with the generated IDs from each side. The junction table has no primary key of its own — it uses a composite key of the two FK columns.

### Insertion Strategy

1. Topologically sort tables based on `belongs_to` dependencies (parents before children)
2. For each validated record:
   a. Insert into independent tables first, capture returned IDs
   b. Resolve `belongs_to` FK values using the lookup cache
   c. Insert into dependent tables with FK references
   d. Insert junction rows linking related records
3. Commit entire batch as one transaction (rollback on any failure — no partial inserts)

### Table Creation

- **Default:** Require tables to already exist. Fail fast with clear error if not.
- **`--create-tables` flag:** Opt-in to `Base.metadata.create_all(engine)`. SQL types are derived from the built-in field types (see the SQL Type column in the Built-in Field Types table).

### Database Compatibility

Siphon targets any SQL database supported by SQLAlchemy. The primary tested databases are MySQL/MariaDB, PostgreSQL, and SQLite. Dialect-specific behavior (e.g., SQLite's limited ALTER TABLE, auto-increment differences) is handled by SQLAlchemy's dialect layer. Users are responsible for installing the appropriate database driver (e.g., `pymysql`, `psycopg2`, `aiosqlite`).

## Deduplication

### Configuration

```yaml
deduplication:
  key: [company_name]              # fields to match on
  check_db: true                   # also check existing DB rows
  match: case_insensitive          # exact | case_insensitive
```

### Behavior

1. **Batch deduplication:** Before insertion, Siphon builds a set of seen values from the dedup key fields. If a record's key matches an already-seen record in the same batch, it is skipped with a warning log.
2. **Database deduplication** (if `check_db: true`): At pipeline startup, Siphon queries the target table for existing values of the dedup key columns. These are added to the seen set. Records matching existing DB rows are skipped.
3. **Match mode:** `case_insensitive` lowercases both sides before comparison. `exact` compares as-is.
4. **Composite keys:** When `key` contains multiple fields, all fields must match for a record to be considered a duplicate.

## HITL Review Engine

### API Layer (`core/reviewer.py`)

```python
class ReviewBatch:
    records: list[dict]
    status: ReviewStatus       # pending, approved, rejected

    def __init__(self, records, llm_client, config): ...
    def approve() -> None
    def reject() -> None
    async def revise(command: str) -> ReviewBatch  # calls LLM, re-validates
    def get_summary() -> dict
    def get_sql_preview() -> list[str]
```

The `ReviewBatch` receives the LLM client and config via its constructor for revision support.

### CLI Presentation (v1)

- Display record tree grouped by relationships
- Show summary (record count, tables affected)
- Show SQL preview
- Accept: `approve`, `reject`, or natural language revision command
- Multiple revision rounds allowed

The `ReviewBatch` API is presentation-agnostic — a future web UI would consume the same API.

## Error Handling

### Exception Hierarchy

```
SiphonError (base)
├── ConfigError          — invalid YAML, missing required fields, unknown types
├── ExtractionError      — LLM call failed, unparseable response, row count mismatch
├── ValidationError      — record failed Pydantic validation
├── DatabaseError        — connection failed, insert failed, table doesn't exist
└── ReviewError          — revision failed, invalid review action
```

### Failure Behavior

| Failure | Behavior |
|---------|----------|
| LLM extraction fails on a chunk | Retry once. If still fails, log, skip chunk, continue. Report skipped chunks at end. |
| LLM returns wrong row count | Retry once with correction prompt. If still wrong, skip chunk. |
| Validation fails on a record | Log, exclude record, continue. Report exclusion count. |
| DB insertion fails | Rollback entire transaction. No partial inserts. |
| Config is invalid | Fail fast at startup with clear error pointing to the problem. |

### Logging

- **Main log:** `siphon_YYYYMMDD_HHMMSS.log` — pipeline progress, warnings, errors
- **SQL log:** `siphon_sql.log` — every SQL statement executed (rotating, 5MB max)
- **Log level:** Configurable via YAML `pipeline.log_level` or `--verbose`/`--quiet` flags

## Dependencies

```toml
dependencies = [
    "openai>=1.0.0",
    "sqlalchemy[asyncio]>=2.0.0",
    "pydantic>=2.0.0",
    "pandas>=2.0.0",
    "openpyxl>=3.0.0",
    "odfpy>=1.4",
    "typer>=0.9.0",
    "pyyaml>=6.0",
    "python-dotenv>=1.0.0",
    "rich>=13.0.0",
]
```

**Removed from current project:** `llama-index`, `nest_asyncio`, `mysql-connector-python`
**Added:** `typer`, `pyyaml`, `rich`

Users install their own database driver (e.g., `pip install pymysql`, `pip install aiosqlite`).

## Testing Strategy

### Unit Tests
- **Field type formatters** — each built-in type with valid, invalid, and edge-case inputs
- **Config loader** — valid YAML, invalid YAML, missing fields, env var substitution
- **Prompt generation** — verify prompts are correctly built from schema definitions
- **Deduplication logic** — exact match, case-insensitive, composite keys, batch + DB

### Integration Tests
- **LLM extraction** — mock OpenAI-compatible server, verify JSON parsing and row count handling
- **Dynamic ORM generation** — verify models are correctly built from config, using SQLite in-memory
- **Insertion with relationships** — belongs_to resolution, junction table creation, topological sort
- **Full pipeline** — end-to-end with mock LLM and SQLite, from spreadsheet to DB

### Acceptance Criteria
- The example use case (company + address + parent hierarchy + junction table) can be fully replicated via a Siphon YAML config
- `siphon validate` catches all config errors before runtime
- `--dry-run` completes without touching the database
- HITL review cycle (approve, reject, revise) works correctly

## Migration: Example Config

The example `company_uploader` use case is replicated by this config:

```yaml
name: "example-companies"

llm:
  base_url: "https://api.openai.com/v1"
  model: "gpt-4o"
  api_key: "${OPENAI_API_KEY}"
  extraction_hints: |
    Priority for company name: direct name column > parent + city > placeholder.
    If no company name can be determined, use "[LocationNameRequired][City]".

database:
  url: "mysql+pymysql://${MYSQL_USER}:${MYSQL_PASSWORD}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"

schema:
  fields:
    - name: company_name
      type: string
      required: true
      min_length: 2
      db: { table: ws_company, column: name }
    - name: parent_entity
      type: string
      db: { table: ws_company, column: parent_name }
    - name: phone
      type: phone
      db: { table: ws_company, column: phone }
    - name: website
      type: url
      db: { table: ws_company, column: website_url }
    - name: address
      type: string
      min_length: 5
      db: { table: ws_address, column: address }
    - name: state
      type: enum
      preset: us_states
      db: { table: ws_address, column: state }

  tables:
    ws_company:
      primary_key: { column: id, type: auto_increment }
    ws_address:
      primary_key: { column: id, type: auto_increment }

  deduplication:
    key: [company_name]
    check_db: true
    match: case_insensitive

relationships:
  - type: belongs_to
    field: parent_entity
    table: ws_company
    references: ws_company
    fk_column: parent_company_id
    resolve_by: name
  - type: junction
    link: [ws_company, ws_address]
    through: ws_company_address
    columns:
      ws_company: company_id
      ws_address: address_id

pipeline:
  chunk_size: 25
  review: true
```

This serves as both a migration guide and a real-world example config for documentation.

## Out of Scope (v1)

- Web UI for HITL review
- Python escape hatches / custom transforms
- NoSQL database support
- Interactive Q&A mode
- Non-spreadsheet data sources (API, database-to-database)
- `percentage` and `text` field types
- Multi-sheet processing (only first sheet or `--sheet` flag)
