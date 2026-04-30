# Siphon — Configurable ETL Pipeline

Siphon is a YAML-driven ETL pipeline that loads spreadsheets and XML/JSON files, maps columns to a target schema, validates records, and inserts them into any SQLAlchemy-supported database. An optional human-in-the-loop review step lets you approve or reject records before they are committed.

No LLM required — field mapping is declared directly in the config file.


## Installation

Requires Python 3.11 or later.

```
pip install siphon-etl
```

To use async database drivers (recommended):

```
# SQLite (for development/testing)
pip install aiosqlite

# PostgreSQL
pip install asyncpg

# MySQL
pip install aiomysql
```


## Quickstart

**1. Generate a starter config:**

```
siphon init
```

This creates `siphon.yaml` in the current directory. Open it and configure your database, source, and schema.

**2. Validate your config:**

```
siphon validate
```

**3. Run the pipeline:**

```
siphon run data.csv --create-tables
```

Siphon will map, validate, and (after optional review) insert the records.


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
