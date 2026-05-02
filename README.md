# Siphon

Configurable, YAML-driven ETL for spreadsheets and XML. Map source columns to your database schema, validate, dedupe, and insert — all with one config file and zero Python code (most of the time).

## Quickstart

You have a CSV. You want it in your database. Here's the 5-minute walkthrough.

### 1. Install

```bash
pip install siphon-etl
# Plus your DB driver of choice:
pip install aiosqlite          # for SQLite (testing)
pip install asyncpg            # for PostgreSQL
pip install aiomysql           # for MySQL
```

### 2. Generate a starter config

```bash
siphon init
```

This writes a heavily-commented `siphon.yaml` to your current directory. Open it.

### 3. Configure your import

The config has three sections you'll edit:

**Source** — what file to read:

```yaml
source:
  type: spreadsheet     # or 'xml'
```

**Database** — where to write:

```yaml
database:
  url: "postgresql+asyncpg://user:pass@localhost/mydb"
```

**Schema** — how to map source columns to database tables:

```yaml
schema:
  fields:
    - name: company_name
      source: "Company Name"        # the column header in your CSV
      type: string
      required: true
      db:
        table: companies            # the database table to insert into
        column: name                # the database column to use
  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment
```

### 4. Validate the config

```bash
siphon validate
```

Catches typos and structural errors without touching the database. Fix any errors it reports.

### 5. Run a dry run

```bash
siphon run data.csv --dry-run
```

Loads the source, validates each record, and shows you what WOULD be inserted — without writing anything. Use this to sanity-check before committing.

### 6. Run for real

```bash
siphon run data.csv --create-tables
```

`--create-tables` auto-creates any missing tables. Drop it once your schema is stable.

That's it. For more, see the cookbook below or the reference sections.

---

## Cookbook

Real-world configs that demonstrate common patterns. Copy these, adapt the field names, and you're 80% done.

### Recipe 1: Import a CSV of customers (with deduplication)

`customers.yaml`:

```yaml
name: customer-import
source:
  type: spreadsheet
database:
  url: "${DATABASE_URL}"

schema:
  fields:
    - name: email
      source: "Email"
      aliases: ["Email Address", "E-mail"]
      type: email
      required: true
      db: { table: customers, column: email }
    - name: full_name
      source: "Name"
      aliases: ["Full Name", "Customer Name"]
      type: string
      required: true
      db: { table: customers, column: name }
    - name: phone
      source: "Phone"
      type: phone
      db: { table: customers, column: phone }

  tables:
    customers:
      primary_key: { column: id, type: auto_increment }

  deduplication:
    key: [email]
    check_db: true              # also check existing DB rows
    match: case_insensitive

pipeline:
  review: false
```

Run: `siphon run customers.csv --create-tables`

### Recipe 2: Import customers + addresses from two CSVs (multi-source join)

`customers_with_addresses.yaml`:

```yaml
name: customers-with-addresses

sources:
  - name: customers
    type: spreadsheet
    path: "./customers.csv"
    fields:
      - name: full_name
        source: "Name"
        type: string
        required: true
        db: { table: customers, column: name }
      - name: customer_code
        source: "Code"
        type: string

  - name: addresses
    type: spreadsheet
    path: "./addresses.csv"
    fields:
      - name: address
        source: "Address"
        type: string
        db: { table: addresses, column: street }
      - name: customer_code
        source: "Customer Code"
        type: string

joins:
  - left: customers
    right: addresses
    "on": customer_code           # bare 'on' is a YAML keyword — quote it
    type: left

database:
  url: "${DATABASE_URL}"

schema:
  tables:
    customers: { primary_key: { column: id, type: auto_increment } }
    addresses: { primary_key: { column: id, type: auto_increment } }
  deduplication:
    key: [full_name]
    match: case_insensitive

relationships:
  - type: junction
    link: [customers, addresses]
    through: customer_addresses
    columns:
      customers: customer_id
      addresses: address_id

pipeline:
  review: false
```

Run: `siphon run --config customers_with_addresses.yaml --create-tables`

(Note: with `sources:`, the CLI input path is optional — paths come from YAML.)

### Recipe 3: Idempotent re-import with upserts (safe to re-run)

When you receive an updated CSV every week and want to upsert (insert new rows, update existing ones):

```yaml
schema:
  tables:
    customers:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [email]              # match existing rows by email
        action: update            # update | skip | error
        update_columns: all       # all | [list of columns]
```

Now `siphon run new_data.csv` is idempotent — running it twice produces the same DB state as running it once.

---

## Source Types

| Type          | Description                              | File formats           |
|---------------|------------------------------------------|------------------------|
| `spreadsheet` | Tabular data from CSV or Excel files     | `.csv`, `.xlsx`, `.xls`, `.ods` |
| `xml`         | Hierarchical XML records                 | `.xml`                 |
| `json`        | JSON array or nested structure           | `.json`                |

For XML and JSON sources, use `root` to specify a dot-path to the list of records (e.g. `"Records.Item"`).


## Field Types

All 14 supported field types:

| Type          | Description                                              | Options                          |
|---------------|----------------------------------------------------------|----------------------------------|
| `string`      | Text, whitespace stripped                                | `min_length`, `max_length`       |
| `integer`     | Whole number                                             | `min`, `max`                     |
| `number`      | Floating-point number                                    | `min`, `max`                     |
| `currency`    | Decimal amount, strips `$` and commas                    | —                                |
| `phone`       | US phone number, formatted as `(NXX) NXX-XXXX`          | —                                |
| `url`         | URL, prepends `http://` if scheme is missing             | —                                |
| `email`       | Email address, lowercased                                | —                                |
| `date`        | Date parsed from flexible input                          | `format` (strftime string)       |
| `datetime`    | Datetime parsed from flexible input                      | `format` (strftime string)       |
| `enum`        | One of an explicit list or a named preset                | `values`, `preset`, `case`       |
| `boolean`     | True/false from `yes/no`, `true/false`, `1/0`            | —                                |
| `regex`       | String validated against a regular expression            | `pattern` (required)             |
| `subdivision` | ISO 3166-2 subdivision code (state, province, etc.)      | `country_code` (required)        |
| `country`     | ISO 3166-1 alpha-2 country code                          | —                                |

**Enum presets:** `us_states` (US states and territories), `ca_provinces` (Canadian provinces and territories).


## Upserts

By default, inserting a row that conflicts with an existing unique key raises an error. To enable insert-or-update behavior, declare an `on_conflict` policy on a table:

```yaml
schema:
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [name]              # field names that form the unique conflict key
        action: update           # update | skip | error (default: error)
        update_columns: all      # all | [list of column names]
```

**Actions:**
- `update` — update the existing row with new values (true upsert)
- `skip` — silently keep the existing row, ignore the new one
- `error` — fail the transaction (default)

**Composite keys:** `key` accepts multiple field names. All must match for a row to be considered a conflict.

**Selective updates:** `update_columns` defaults to `all` (every non-key column). Provide a list to update only specific columns; others are preserved from the existing row.

**Database support:** Native upserts on PostgreSQL, MySQL, MariaDB, and SQLite (3.24+). For other dialects, Siphon falls back to a non-atomic select-then-update path — concurrent writers may cause unique constraint violations on the fallback path.


## Dry-Run with Diff

When you run `siphon run --dry-run`, Siphon shows what *would* change before committing anything to the database:

```
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
```

**Categories:**
- `Insert` — new rows that would be added
- `Update` — existing rows whose values would change
- `Skip` — existing rows that would be skipped (when `on_conflict.action: skip`)
- `No Change` — rows that already match the database

For scripting, use `--output json` to get machine-readable output:

```bash
siphon run data.csv --dry-run --output json
```

The diff respects the `on_conflict.key` declared on each table. If no `on_conflict` is configured, every record is categorized as `Insert`.


## Resumable Runs

When importing large datasets, a failure mid-import doesn't mean starting over. Siphon tracks each run in a `_siphon_runs` metadata table inside the target database, and `--resume` continues from where the last failure stopped.

```bash
# Initial run fails partway through (e.g., network blip, constraint violation)
siphon run big_data.csv

# Fix whatever caused the failure, then resume
siphon run big_data.csv --resume
```

**How it works:**
- Records are inserted in batches (default 500). Each batch commits in its own transaction.
- After each successful batch, Siphon updates `processed_count` in `_siphon_runs`.
- On failure, the failing batch is rolled back; earlier batches stay committed.
- `--resume` finds the most recent failed run for the same pipeline name + source file + config hash, and skips records up to its `processed_count`.

**Configuration:**
```yaml
pipeline:
  batch_size: 500       # records per transaction commit (default 500)
  track_runs: true      # enable _siphon_runs table (default true)
```

**Trade-offs:**
- Batch commits are not atomic across the whole import. If you need all-or-nothing semantics, set `batch_size` larger than your dataset.
- Resume requires upserts or schemas with no unique constraints — otherwise re-processing already-inserted records will fail. Combine with `on_conflict.action: update` for safe resumability.
- Run tracking adds a `_siphon_runs` table to the target database. Disable with `track_runs: false` if you don't want it.
- Resume tracks main records only — collection records (from XML/JSON nested arrays) are re-processed on resume.


## Audit Trail

Every record-level action (insert, update, skip) is logged in a `_siphon_audit` table. Use `--user NAME` to attribute imports to a person:

```bash
siphon run data.csv --user alice
```

Then anyone can answer "what got imported when, and by whom?" with a SQL query:

```sql
SELECT target_table, action, target_pk, reviewed_by, created_at
FROM _siphon_audit
WHERE created_at > '2026-04-01';
```

**What gets logged:**
- Every insert: target table, PK, source file, source row number
- Every update (from upserts): JSON of field-level changes (`{"col": {"old": ..., "new": ...}}`)
- Every skip (when `on_conflict.action: skip`): target table and PK
- The `--user NAME` value (or NULL if not provided)
- The `run_id` from `_siphon_runs` for cross-referencing

**No-op updates are not audited.** If an upsert results in zero field changes, no audit row is written — only meaningful changes are recorded.

**Configuration:**
```yaml
pipeline:
  audit: true       # default — disable with `audit: false`
```

The audit table is only written when both `track_runs` and `audit` are true (the default). Audit writes happen in a separate transaction from the data inserts, so audit failures won't break the import.


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
    "on": company_code
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


## Transforms

Built-in transforms can be applied inline on any field:

| Type        | Description                                             |
|-------------|---------------------------------------------------------|
| `template`  | Build a string from other fields using `{field}` syntax |
| `map`       | Map one value to another via a lookup dictionary        |
| `concat`    | Join multiple fields with a separator                   |
| `uuid`      | Generate a new UUID v4                                  |
| `now`       | Insert the current timestamp                            |
| `coalesce`  | Use the first non-empty value from a list of fields     |

You can also load custom Python transform functions from a file:

```yaml
transforms:
  file: transforms.py
```

```python
# transforms.py
def normalize_code(value, record):
    return value.strip().upper()
```

Then reference it on a field:

```yaml
transform:
  type: custom
  function: normalize_code
```


## Collections

For nested XML or JSON data, use `collections` to expand child records into separate table rows:

```yaml
schema:
  collections:
    - name: notes
      source_path: "Notes.Note"
      fields:
        - name: note_text
          source: text
          type: string
          db:
            table: notes
            column: body
```


## Example Config — Company Import

```yaml
name: "company-import"

source:
  type: spreadsheet

database:
  url: "${DATABASE_URL}"

schema:
  fields:
    - name: company_name
      source: "Company Name"
      type: string
      required: true
      min_length: 2
      db:
        table: companies
        column: name

    - name: phone
      source: "Phone"
      type: phone
      db:
        table: companies
        column: phone_number

    - name: website
      source: "Website"
      type: url
      db:
        table: companies
        column: website_url

    - name: state
      source: "State"
      type: enum
      preset: us_states
      db:
        table: companies
        column: state_code

    - name: founded
      source: "Founded"
      type: date
      format: "%Y-%m-%d"
      db:
        table: companies
        column: founded_date

  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment

  deduplication:
    key: [company_name]
    check_db: true
    match: case_insensitive

pipeline:
  review: false
  log_level: info
```


## Example Config — Incident XML Import

```yaml
name: "incident-import"

source:
  type: xml
  root: "Incidents.Incident"
  encoding: utf-8

database:
  url: "${DATABASE_URL}"

schema:
  fields:
    - name: incident_id
      source: "@id"
      type: string
      required: true
      db:
        table: incidents
        column: external_id

    - name: reported_date
      source: "ReportedDate"
      type: date
      format: "%Y-%m-%d"
      db:
        table: incidents
        column: reported_date

    - name: severity
      source: "Severity"
      type: enum
      values: [low, medium, high, critical]
      case: lower
      db:
        table: incidents
        column: severity

  collections:
    - name: notes
      source_path: "Notes.Note"
      fields:
        - name: note_body
          source: "Body"
          type: string
          db:
            table: incident_notes
            column: body

  tables:
    incidents:
      primary_key:
        column: id
        type: auto_increment
    incident_notes:
      primary_key:
        column: id
        type: uuid

pipeline:
  review: false
  log_level: info
```


## CLI Reference

### `siphon run <input_path>`

Execute the full ETL pipeline.

| Flag              | Default        | Description                                           |
|-------------------|----------------|-------------------------------------------------------|
| `--config`, `-c`  | `siphon.yaml`  | Path to YAML config file                              |
| `--create-tables` | off            | Auto-create tables if they do not exist               |
| `--dry-run`       | off            | Map and validate only, skip DB insertion              |
| `--no-review`     | off            | Skip human-in-the-loop review, insert directly        |
| `--sheet`         | first sheet    | Sheet name or index for multi-sheet Excel files       |
| `--verbose`, `-v` | off            | Set log level to `debug`                              |
| `--quiet`, `-q`   | off            | Set log level to `error`                              |

`<input_path>` can be a single file (`data.csv`, `data.xlsx`, `data.xml`) or a directory. When a directory is given, all matching source files inside it are processed in sequence.

### `siphon validate`

Validate a config file without running the pipeline.

| Flag             | Default       | Description              |
|------------------|---------------|--------------------------|
| `--config`, `-c` | `siphon.yaml` | Path to YAML config file |

### `siphon init`

Generate a starter `siphon.yaml` in the current directory. Prompts before overwriting an existing file.

### `siphon --version`

Print the installed Siphon version and exit.


## Supported Databases

Any database with an async SQLAlchemy driver:

| Database   | URL format                                         | Driver       |
|------------|----------------------------------------------------|--------------|
| SQLite     | `sqlite+aiosqlite:///path/to/db.sqlite`            | `aiosqlite`  |
| PostgreSQL | `postgresql+asyncpg://user:pass@host/dbname`       | `asyncpg`    |
| MySQL      | `mysql+aiomysql://user:pass@host/dbname`           | `aiomysql`   |

Environment variable substitution is supported anywhere in the config via `${VAR_NAME}`. A `.env` file in the current directory is loaded automatically.


## Dependencies

- `sqlalchemy[asyncio]` — async database engine and ORM
- `pydantic` — config validation and record models
- `pandas` / `openpyxl` / `odfpy` — spreadsheet parsing
- `lxml` — XML parsing
- `typer` / `rich` — CLI and terminal output
- `pyyaml` / `python-dotenv` — config loading
- `pycountry` — enum presets for country and subdivision fields
- `python-dateutil` — flexible date/datetime parsing
