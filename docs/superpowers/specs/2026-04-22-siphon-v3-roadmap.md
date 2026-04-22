# Siphon v3 Roadmap — Production-Grade ETL Features

**Date:** 2026-04-22
**Status:** Approved (roadmap level)
**Builds on:** v2 (no-LLM, sources + transforms + collections)

## Overview

Siphon v2 replaced the LLM with explicit YAML-driven mapping, making it deterministic and free. Siphon v3 adds the features that elevate it from "a 50-line script alternative" to "a production-grade ETL tool worth maintaining and adopting."

The roadmap targets open-source adoption. Each feature solves a real friction point identified through actual usage.

## Sequencing Rationale

| Phase | Feature | Depends On | Unlocks |
|-------|---------|------------|---------|
| 1 | Upserts | — | Resumable runs, dry-run diff |
| 2 | Dry-run with diff | Phase 1 (lookup-by-key logic) | Audit trail diff format |
| 3 | Incremental/resumable runs | Phase 1 (handle "already exists") | Audit trail run_id |
| 4 | Audit trail | Phases 1-3 | Web UI audit page |
| 5 | Pre/post hooks | — (independent) | — |
| 6 | Multi-source joins | — (independent) | — |
| 7 | Scheduled/watched imports | Phases 1-4 (reliability) | Service deployment |
| 8 | Web UI | Phases 1-7 (API surface) | Non-technical adoption |

Phases 1-4 build linearly. Phases 5-7 are independent and can be reordered. Phase 8 is the capstone.

---

## Phase 1: Upserts

### Problem

Siphon v2 can only INSERT. Real-world imports almost always need "insert if new, update if changed." Without upserts, re-importing updated data creates duplicates or fails on unique constraint violations.

### Design

Per-table `on_conflict` configuration:

```yaml
schema:
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
      on_conflict:
        key: [name]                    # unique key to match on (composite supported)
        action: update                 # update | skip | error
        update_columns: all            # all | [list of specific columns]
```

**Three actions:**
- `update` — true upsert (update existing row with new values)
- `skip` — silently skip (DB-level dedup)
- `error` — fail the transaction (current default behavior)

**Implementation:** Dialect-agnostic via SQLAlchemy:
- PostgreSQL: `insert().on_conflict_do_update()`
- SQLite: `insert().prefix_with("OR REPLACE")`
- MySQL: `insert().on_duplicate_key_update()`
- Fallback: select-then-update for unsupported dialects (non-atomic — race condition possible if concurrent writers exist; warn users in docs)

### Relationship to Existing Dedup

Dedup and upserts serve different purposes and coexist:
- **Dedup** (in-memory + DB check): "don't try to insert this — it's a duplicate in the batch"
- **Upsert** (DB-level): "this row exists in the DB; update it instead of failing"

A pipeline can use both. Dedup runs first to compress the batch, then upserts handle any remaining DB-level conflicts.

---

## Phase 2: Dry-Run with Diff

### Problem

Current `--dry-run` only reports counts ("10 valid, 0 invalid"). With upserts, users need to see *what* would change before committing.

### Design

Dry-run queries the DB for existing rows matching `on_conflict.key`, compares field values, and reports per-record categorization:

```
Pipeline Diff (dry run)
┌──────────┬───────┐
│ Action   │ Count │
├──────────┼───────┤
│ Insert   │     5 │
│ Update   │     3 │
│ Skip     │     2 │
│ No Change│     1 │
│ Invalid  │     0 │
└──────────┴───────┘

Updates:
  companies.name="Acme Corp" → phone_number: "(555) 123-4567" → "(555) 999-8888"
  companies.name="Beta Inc"  → website_url: "http://beta.io" → "http://beta.com"
```

**Output formats:**
- Rich table (default)
- JSON (`--output json`) for CI/scripting

The diff engine reuses the lookup-by-key logic from Phase 1 — that's why upserts come first.

---

## Phase 3: Incremental/Resumable Runs

### Problem

A 10,000-row import fails at row 6,000. Users must either start over (and duplicate the first 6,000) or manually track where it stopped.

### Design

Run state stored in a metadata table inside the target database (no external state file to lose):

```sql
_siphon_runs:
  id              INTEGER PRIMARY KEY
  pipeline_name   VARCHAR(255)    -- from config name
  source_file     VARCHAR(500)    -- input file path
  started_at      DATETIME
  completed_at    DATETIME NULL   -- NULL = incomplete
  status          VARCHAR(20)     -- running | completed | failed
  total_records   INTEGER
  processed_count INTEGER         -- last successfully committed record
  error_message   TEXT NULL
  config_hash     VARCHAR(64)     -- SHA256 of config, detects mid-run changes
```

**Behavior:**
1. Before insertion: write a `running` row to `_siphon_runs`
2. Records inserted in batches (configurable size, default 500), committed per-batch
3. After each batch: update `processed_count`
4. On success: status → `completed`
5. On failure: status → `failed`, save `error_message`
6. On `--resume`: find the last `failed` run for this pipeline+source, skip first N records, continue

### Trade-off: Atomicity vs. Recoverability

v2 commits the entire batch as one transaction. v3's batch-level commits sacrifice all-or-nothing atomicity for the ability to resume. Users who want the old behavior can set `batch_size` larger than their dataset, or set `track_runs: false`.

### New Flags

- `--resume` — continue from the last failed run for this pipeline+source
- `--batch-size N` — records per commit (default 500)

### Config

```yaml
pipeline:
  batch_size: 500
  track_runs: true
```

---

## Phase 4: Audit Trail

### Problem

Compliance use cases (HR, legal, incident reporting) need to answer "what got imported when, by whom?" No built-in answer today.

### Design

Per-record action log in `_siphon_audit`:

```sql
_siphon_audit:
  id              INTEGER PRIMARY KEY
  run_id          INTEGER         -- FK to _siphon_runs.id
  target_table    VARCHAR(255)
  target_pk       VARCHAR(255)    -- inserted/updated row's PK value
  action          VARCHAR(10)     -- insert | update | skip
  field_changes   TEXT NULL       -- JSON: {"phone": {"old": "...", "new": "..."}}
  source_file     VARCHAR(500)
  source_row      INTEGER NULL    -- row number in source
  reviewed_by     VARCHAR(255) NULL  -- from HITL approval
  created_at      DATETIME
```

**What gets logged:**
- Inserts: target table, PK, source row
- Updates (from upserts): which fields changed with old→new values
- Skips: reason (duplicate, no-change, rejected in review)
- Review approvals: who approved (from `--user` flag or system username)

**New flag:** `--user NAME` — identifies who's running the import

**Config:** `pipeline.audit: true` (default)

**Querying:** No built-in UI in this phase — users query `_siphon_audit` directly. Web UI (Phase 8) adds browsable views.

---

## Phase 5: Pre/Post Hooks

### Problem

Users manually run shell commands and SQL around `siphon run` (disable triggers, refresh views, send Slack notifications). Should be declarative.

### Design

```yaml
pipeline:
  hooks:
    pre:
      - type: sql
        run: "SET FOREIGN_KEY_CHECKS = 0"
      - type: shell
        run: "echo 'Starting at $(date)' >> /var/log/imports.log"
    post:
      - type: sql
        run: "REFRESH MATERIALIZED VIEW company_stats"
      - type: shell
        run: "curl -X POST $SLACK_WEBHOOK -d '{\"text\": \"Done\"}'"
    on_error:
      - type: shell
        run: "curl -X POST $SLACK_WEBHOOK -d '{\"text\": \"Failed\"}'"
```

**Three hook points:**
- `pre` — runs before any records processed (after config validation, DB connection verified)
- `post` — runs only after successful completion
- `on_error` — runs when pipeline fails

**Two hook types:**
- `sql` — executed against the target DB connection
- `shell` — subprocess with environment variables: `$SIPHON_PIPELINE_NAME`, `$SIPHON_TOTAL_INSERTED`, `$SIPHON_RUN_ID`, etc.

**Failure semantics:**
- `pre` hook failure: aborts pipeline
- `post`/`on_error` hook failure: warning logged, pipeline status unchanged (data already committed)

Hooks run sequentially in declared order.

---

## Phase 6: Multi-Source Joins

### Problem

Companies in one CSV, addresses in another, joined by a shared key. Currently requires separate runs and manual FK handling.

### Design

Multiple named sources, joined by key:

```yaml
sources:
  - name: companies
    type: spreadsheet
    fields:
      - name: company_name
        source: "Name"
        db: { table: companies, column: name }
      - name: company_code
        source: "Code"
        db: { table: companies, column: code }

  - name: addresses
    type: spreadsheet
    fields:
      - name: address
        source: "Street Address"
        db: { table: addresses, column: full_address }
      - name: company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    on: company_code
    type: left          # left | inner
```

**Pipeline flow:**
1. Each source loaded and mapped independently
2. Join produces merged records
3. Merged records flow through normal validate → dedup → insert
4. `left` join: unmatched left rows still insert with null right fields
5. `inner` join: drops unmatched records from both sides

### Backward Compatibility

Existing single-source configs continue working. The `source:` (singular) syntax becomes shorthand for a single-item `sources:` list.

### Scope Note

This is the most architecturally complex phase. Introduces join logic, key matching, cardinality handling (one-to-many, many-to-many), and a new config shape. Worth doing, but should be designed carefully when reached.

---

## Phase 7: Scheduled/Watched Imports

### Problem

Manual `siphon run` for every new file. Users want automatic processing of new files in a directory.

### Design

New command:

```bash
siphon watch ./incoming/ --config siphon.yaml --create-tables
```

**Behavior:**
- Polls a directory for new files matching supported extensions
- On new file: runs the full pipeline against it
- On success: moves file to `./incoming/_processed/`
- On failure: moves file to `./incoming/_failed/`
- Each run logged via `_siphon_runs` (Phase 3)

**Config:**

```yaml
pipeline:
  watch:
    poll_interval: 10          # seconds between scans
    processed_dir: _processed
    failed_dir: _failed
    settle_time: 2             # seconds to wait after file stops changing
```

**Implementation:** Polling via `pathlib` + `asyncio.sleep`. No external deps (no watchdog, no inotify). Less efficient but works everywhere — Linux, Mac, Docker, network mounts.

### Out of Scope

- S3/cloud bucket watching (community can request)
- Cron scheduling (use system cron + `siphon run`)
- Daemonization (use systemd/supervisor/Docker)

Philosophy: Siphon handles "what to do with files." OS handles "when to run" and "keep alive."

---

## Phase 8: Web UI

### Problem

CLI review excludes non-technical users. Open-source adoption requires a browser interface. Even developers benefit from a dashboard for run history and audit logs.

### Design

Built-in web UI shipping with `pip install siphon-etl`. Pre-built React assets bundled in the Python package — no npm required for end users.

**New command:**

```bash
siphon server --port 8080
```

Starts FastAPI backend serving both the API and bundled React frontend.

### Pages

| Page | Content |
|------|---------|
| Dashboard | Recent runs, success/fail counts, next scheduled watch |
| Run History | `_siphon_runs` table — status, duration, counts, drill-down |
| Run Detail | Per-record breakdown: inserts, updates, skips. Field-level diffs |
| Review Queue | Pending HITL batches. Approve/reject buttons |
| Audit Log | Browsable `_siphon_audit` with filters (date, table, action, user) |
| Config Viewer | Read-only YAML view (config remains a file) |

### Architecture

```
siphon/
  server/
    api.py          # FastAPI: REST endpoints for runs, audit, review
    static/         # Pre-built React bundle (committed)
  ui/               # React source (separate build, dev only)
    src/
    package.json
```

End users `pip install` and get pre-built assets. Contributors run `npm run build` in `ui/` to regenerate `server/static/`.

### API-First Design

Review Queue uses the existing `ReviewBatch` API. FastAPI endpoints wrap it. CLI review and web review are interchangeable — start in web, finish in CLI.

### Out of Scope

- User authentication (use a reverse proxy)
- Config editing (edit YAML directly)
- Triggering runs from UI (use CLI or watch mode)

---

## Cumulative Impact

| Capability | v1 | v2 | v3 |
|------------|----|----|----|
| Source types | spreadsheet | + XML, JSON | (same) |
| Column mapping | LLM | explicit | (same) |
| Transforms | none | built-in + custom | (same) |
| Collections | none | yes | (same) |
| LLM dependency | required | none | none |
| Insert | yes | yes | yes |
| Upsert (insert/update) | no | no | **yes** |
| Dry-run with diff | no | no | **yes** |
| Resumable runs | no | no | **yes** |
| Audit trail | no | no | **yes** |
| Pre/post hooks | no | no | **yes** |
| Multi-source joins | no | no | **yes** |
| Watched/scheduled | no | no | **yes** |
| Web UI | no | no | **yes** |

---

## Implementation Strategy

Each phase ships as an independent release:
- v3.1: Upserts
- v3.2: Dry-run with diff
- v3.3: Incremental runs
- v3.4: Audit trail
- v3.5: Pre/post hooks
- v3.6: Multi-source joins
- v3.7: Watched imports
- v3.8: Web UI

Each release adds tests, updates README, and is independently usable. Users can adopt v3.1 and ignore the rest if upserts are all they need.
