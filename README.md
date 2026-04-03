# siphon

A configurable, YAML-driven, LLM-powered ETL pipeline for extracting structured data from spreadsheets and loading it into a database.

Siphon reads CSV, Excel, or ODS files, uses an LLM to extract and normalize fields according to your schema, validates the results, and inserts them into any SQLAlchemy-supported database. An optional human-in-the-loop review step lets you approve, reject, or revise records before they are committed.


## Installation

Requires Python 3.11 or later.

```
pip install siphon
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

This creates `siphon.yaml` in the current directory. Open it and configure your LLM, database, and schema.

**2. Validate your config:**

```
siphon validate
```

**3. Run the pipeline:**

```
siphon run data.csv
```

Siphon will extract, validate, and (after optional review) insert the records.


## Example Config

```yaml
name: "company-pipeline"

llm:
  base_url: "http://localhost:11434/v1"   # Ollama, OpenAI, vLLM, LM Studio
  model: "llama3"
  api_key: ""                             # Required for OpenAI

database:
  url: "${DATABASE_URL}"                  # Environment variable substitution supported

schema:
  fields:
    - name: company_name
      type: string
      required: true
      min_length: 2
      db:
        table: companies
        column: name

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

    - name: state
      type: enum
      preset: us_states
      db:
        table: companies
        column: state_code

    - name: founded
      type: date
      format: "%Y-%m-%d"
      db:
        table: companies
        column: founded_date

  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment    # auto_increment | uuid

  # Optional: deduplicate on key fields
  deduplication:
    key: [company_name]
    check_db: true
    match: case_insensitive     # exact | case_insensitive

pipeline:
  chunk_size: 25      # Rows per LLM batch
  review: true        # Enable human-in-the-loop review
  log_level: info     # debug | info | warning | error
```


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


## CLI Reference

### `siphon run <input_path>`

Execute the full ETL pipeline.

| Flag              | Default        | Description                                           |
|-------------------|----------------|-------------------------------------------------------|
| `--config`, `-c`  | `siphon.yaml`  | Path to YAML config file                              |
| `--create-tables` | off            | Auto-create tables if they do not exist               |
| `--dry-run`       | off            | Extract and validate only, skip DB insertion          |
| `--no-review`     | off            | Skip human-in-the-loop review, insert directly        |
| `--chunk-size N`  | from config    | Override number of rows per LLM batch                 |
| `--sheet`         | first sheet    | Sheet name or index for multi-sheet Excel files       |
| `--verbose`, `-v` | off            | Set log level to `debug`                              |
| `--quiet`, `-q`   | off            | Set log level to `error`                              |

`<input_path>` can be a single file (`data.csv`, `data.xlsx`, `data.ods`) or a directory. When a directory is given, all spreadsheet files inside it are processed in sequence.

### `siphon validate`

Validate a config file without running the pipeline.

| Flag             | Default       | Description             |
|------------------|---------------|-------------------------|
| `--config`, `-c` | `siphon.yaml` | Path to YAML config file |

### `siphon init`

Generate a starter `siphon.yaml` in the current directory. Prompts before overwriting an existing file.

### `siphon --version`

Print the installed siphon version and exit.


## Supported Databases

Any database with an async SQLAlchemy driver:

| Database   | URL format                                         | Driver       |
|------------|----------------------------------------------------|--------------|
| SQLite     | `sqlite+aiosqlite:///path/to/db.sqlite`            | `aiosqlite`  |
| PostgreSQL | `postgresql+asyncpg://user:pass@host/dbname`       | `asyncpg`    |
| MySQL      | `mysql+aiomysql://user:pass@host/dbname`           | `aiomysql`   |

Environment variable substitution is supported anywhere in the config via `${VAR_NAME}`. A `.env` file in the current directory is loaded automatically.


## Dependencies

- `openai` — LLM client (OpenAI-compatible API)
- `sqlalchemy[asyncio]` — async database engine and ORM
- `pydantic` — config validation and record models
- `pandas` / `openpyxl` / `odfpy` — spreadsheet parsing
- `typer` / `rich` — CLI and terminal output
- `pyyaml` / `python-dotenv` — config loading
- `pycountry` — enum presets for country and subdivision fields
- `python-dateutil` — flexible date/datetime parsing
