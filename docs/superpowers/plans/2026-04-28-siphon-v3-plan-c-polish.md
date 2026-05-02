# Siphon v3 Plan C: User-Facing Polish

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Siphon feel like a polished tool for first-time users by overhauling error messages and rewriting the README from a feature checklist into a user journey.

**Architecture:** No code logic changes. Pure quality-of-life work. Walk every `raise SiphonError(...)` and improve the message. Rewrite the README. Add a small cookbook of realistic configs.

**Tech Stack:** No new dependencies.

---

## Scope Decision: What's NOT in This Plan

The original Plan C also proposed **bulk insert** as a perf win. After thinking through the implementation, it's dropped from this plan because:

- The current per-record insertion model is required for FK resolution (each record's parent PK must be known before the child inserts).
- Refactoring to per-table bulk insert would require restructuring the entire Inserter to track record→PK mappings across the batch, then build child rows with resolved FKs.
- It would interact with upserts, audit emission, and self-referential records in non-obvious ways.
- The audit rated this "medium value" — meaningful but not a no-brainer.
- Real-world usage data should drive this work. If users complain about throughput on 100k+ row imports, then it's worth the complexity.

**If you want bulk insert later:** it's a meaningful project on its own. Open a Plan D for it once there's a real perf complaint.

---

## What This Plan DOES

| Item | Why |
|------|-----|
| **Audit + improve every `raise SiphonError(...)`** | Single biggest difference between "tool I tolerate" and "tool I recommend." Cheap, high-impact. |
| **Rewrite README as a user journey** | The current README is a feature checklist. New users need a "what do I do first?" walkthrough. |
| **Add cookbook with 3 realistic configs** | Concrete examples reduce the "where do I start?" friction. |
| **Bump version to 0.3.0** (drop alpha) | After 6 phases (1, 2, 3, 4, 6) + Plans A/B/C, v0.3.0 is shippable. |

---

## File Structure

| File | Responsibility | Status |
|------|---------------|--------|
| `siphon/config/loader.py` | Improve `ConfigError` messages | Modify |
| `siphon/config/schema.py` | (Pydantic errors are already good — minor tweaks if needed) | Modify if needed |
| `siphon/sources/spreadsheet.py`, `xml.py` | Improve `SourceError` messages | Modify |
| `siphon/core/mapper.py` | Improve `TransformError` messages | Modify |
| `siphon/core/validator.py` | Improve `ValidationError` messages | Modify if needed |
| `siphon/db/inserter.py` | Improve `DatabaseError` messages | Modify |
| `README.md` | Full rewrite | Modify |
| `docs/cookbook/` | NEW: 3 realistic config examples | Create |
| `siphon/__init__.py` | Bump version to `0.3.0` | Modify |

**No test deletions.** This plan should leave the test count at 994.

---

## Branch

Cut `v3-plan-c-polish` from `v3-plan-b-test-quality`.

---

## Task Breakdown

### Task 1: Branch + baseline

**Files:** branch only

- [ ] **Step 1: Cut a new branch**

```bash
cd /Users/troysparks/Dev/siphon
git checkout v3-plan-b-test-quality
git pull
git checkout -b v3-plan-c-polish
```

- [ ] **Step 2: Verify the baseline**

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `994 passed`. Lock this — every later task ends with the same count.

- [ ] **Step 3: No commit yet**

---

### Task 2: Audit and improve all `raise *Error(...)` calls

**Files:**
- Modify: `siphon/config/loader.py`
- Modify: `siphon/sources/spreadsheet.py`
- Modify: `siphon/sources/xml.py`
- Modify: `siphon/core/mapper.py`
- Modify: `siphon/db/inserter.py`
- Modify: `siphon/db/upsert_executor.py` (any error-raising paths)
- Modify: `siphon/transforms/loader.py`

The goal: every error a user can hit should answer three questions in its message:
1. **What happened?** (e.g., "Failed to read file")
2. **Where?** (file path, field name, table name, source name)
3. **What now?** (suggestion or pointer to docs)

**Existing tests verify the error TYPE is correct (e.g., `pytest.raises(ConfigError)`). Changing the message doesn't break those tests as long as `match=` patterns still apply. Run the test suite after each file's changes.**

#### Step 1: Audit pass — find every `raise`

```bash
cd /Users/troysparks/Dev/siphon && grep -rn "raise.*Error" siphon/ --include="*.py"
```

Read EVERY `raise` site. For each, note the current message and what improvements would help. This is reading-heavy work — there are likely 30-50 raise sites.

#### Step 2: Improve `ConfigError` messages in `siphon/config/loader.py`

Read the current loader. Common error sites:

a. **YAML parse error** — currently might say `"Failed to parse YAML: <error>"`. Improve to:
```python
raise ConfigError(
    f"Failed to parse YAML config at {path}: {e}\n"
    f"Common causes: indentation errors, unquoted special characters, or missing colons."
) from e
```

b. **Missing env var in `${...}` substitution** — currently might say `"Environment variable VAR not set"`. Improve to:
```python
raise ConfigError(
    f"Environment variable '{var_name}' is referenced in {path} but is not set. "
    f"Set it (e.g., export {var_name}=...) or add it to a .env file in the same directory as the config."
) from e
```

c. **Pydantic validation failure** — currently propagates raw Pydantic errors. Wrap with file context:
```python
raise ConfigError(
    f"Invalid config at {path}:\n{exc}"
) from exc
```

d. **Cross-validation errors** (the `_validate_*` helpers from Plan A Task 4):
- Field type errors: include the field name AND what's missing.
  ```python
  raise ConfigError(
      f"Field '{field.name}' has type 'enum' but is missing both 'values' and 'preset'. "
      f"Add either: values: [A, B, C] OR preset: us_states"
  )
  ```
- on_conflict.key references unknown field: include the table name AND the available field names.
  (The existing message already does this — verify it's still good.)
- join references unknown source: same treatment.

For each `ConfigError` you encounter, ask: "if a stranger saw only this message, could they fix it?" If not, improve it.

After changes, run config tests:
```bash
.venv/bin/pytest tests/config/ -q 2>&1 | tail -3
```

If any test fails because of `match=` regex changes, update those tests' regex to be looser (e.g., `match="enum.*missing"` instead of `match="enum.*values.*preset"`).

#### Step 3: Improve `SourceError` messages in `siphon/sources/spreadsheet.py` and `siphon/sources/xml.py`

Read both files. Common error sites:

a. **File not found**:
```python
raise SourceError(
    f"Source file not found: {path}. "
    f"Check the file path is correct and the file exists."
)
```

b. **Unsupported file format**:
```python
raise SourceError(
    f"Unsupported source format: '{ext}' for file {path}. "
    f"Supported formats: .csv, .xlsx, .xls, .ods (spreadsheet); .xml (xml)."
)
```

c. **Pandas parse error** (CSV/XLSX):
```python
raise SourceError(
    f"Failed to read {path}: {e}. "
    f"Check the file isn't corrupted or open in another program."
) from e
```

d. **XML parse error**:
```python
raise SourceError(
    f"Failed to parse XML at {path}: {e}. "
    f"Verify the file is well-formed XML."
) from e
```

e. **XML root path not found**:
```python
raise SourceError(
    f"Root path '{self._root}' not found in {path}. "
    f"Check the 'root' value in your config matches the XML structure."
)
```

After changes:
```bash
.venv/bin/pytest tests/sources/ -q 2>&1 | tail -3
```

Update test regex if needed.

#### Step 4: Improve `TransformError` messages in `siphon/core/mapper.py` and `siphon/transforms/loader.py`

Common error sites:

a. **Custom transform function not found**:
```python
raise TransformError(
    f"Custom transform function '{transform.function}' is referenced in field '{field.name}' "
    f"but is not defined in the transforms file. "
    f"Add 'def {transform.function}(...):' to your transforms.py."
)
```

b. **Unknown transform type**:
```python
raise TransformError(
    f"Unknown transform type: '{transform.type}'. "
    f"Built-in types: template, map, concat, uuid, now, coalesce, custom."
)
```

c. **Transform file not found** (in `transforms/loader.py`):
```python
raise TransformError(
    f"Transforms file not found: {path}. "
    f"Check the 'transforms.file' path in your config."
)
```

After changes:
```bash
.venv/bin/pytest tests/core/test_mapper.py tests/transforms/ tests/core/test_mapper_errors.py -q 2>&1 | tail -3
```

#### Step 5: Improve `DatabaseError` messages in `siphon/db/inserter.py`

Common error sites:

a. **Insert failed** (existing message likely says `"Insert failed at record N: <error>"` — IMPROVE to include the table name AND first-failing record context):
```python
raise DatabaseError(
    f"Insert failed at record {inserted_count + 1} of {len(records)} "
    f"(into table '{table_name}'): {e}. "
    f"This batch was rolled back; previously committed batches are unaffected."
) from e
```

(NOTE: `table_name` may not be available at the catch site — if it isn't, omit it.)

b. **Topological sort cycle detected** (the existing message says `"Circular dependency detected in table relationships"`). Improve:
```python
raise DatabaseError(
    f"Circular dependency in table relationships: {e}. "
    f"Check your `relationships:` config for a cycle (e.g., A → B → A)."
) from e
```

After changes:
```bash
.venv/bin/pytest tests/db/ -q 2>&1 | tail -3
```

#### Step 6: Final verification

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `994 passed`. If any test failed because a `match=` regex no longer applies, EITHER update the test (preferred — make the regex looser) OR adjust the message.

#### Step 7: Commit

```bash
git add siphon/ tests/
git commit -m "feat: improve error messages with context and remediation hints"
```

---

### Task 3: Rewrite README as a user journey

**Files:**
- Modify: `README.md`

The current README is a feature checklist organized around what Siphon CAN do. New users need a "what do I do first?" walkthrough.

#### Step 1: Read the current README

```bash
cat /Users/troysparks/Dev/siphon/README.md
```

Note what's there. PRESERVE: the feature reference sections (field types, transforms, etc.) — these are valuable as reference. ADD a tutorial-style intro at the top.

#### Step 2: Rewrite the top of README.md to lead with a tutorial

Replace whatever is currently between the title and the first `## ...` section with this user-journey opener:

```markdown
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
```

The rest of the README (existing reference sections — field types, transforms, collections, multi-source joins, upserts, dry-run with diff, resumable runs, audit trail) STAYS BELOW this opener. Don't delete those sections — they're valuable as reference once the user understands the basics.

#### Step 3: Add a "Cookbook" section above the reference sections

After the new Quickstart section, insert this cookbook section:

```markdown
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
```

#### Step 4: Verify the README still has the existing reference sections

Run:
```bash
grep -n "^## " /Users/troysparks/Dev/siphon/README.md
```

The output should include (in any order):
- `## Quickstart`
- `## Cookbook`
- `## Source Types` (existing reference)
- `## Field Types` (existing reference)
- `## Upserts` (existing reference)
- `## Dry-Run with Diff` (existing reference)
- `## Resumable Runs` (existing reference)
- `## Audit Trail` (existing reference)
- `## Multi-Source Joins` (existing reference)
- `## Transforms` (existing reference)
- `## Collections` (existing reference)
- `## CLI Reference` (existing reference, if it exists)

If any of those reference sections are missing, restore them from the previous version.

#### Step 5: Commit

```bash
git add README.md
git commit -m "docs: rewrite README with user-journey quickstart and cookbook"
```

---

### Task 4: Bump version to 0.3.0 + push

**Files:**
- Modify: `siphon/__init__.py`

The codebase has shipped 6 phases (1, 2, 3, 4, 6) plus Plans A, B, C. The 0.3.0a* alpha series ends here.

#### Step 1: Bump version

In `siphon/__init__.py`, change:

```python
__version__ = "0.3.0a5"
```

to:

```python
__version__ = "0.3.0"
```

#### Step 2: Run full suite

```bash
.venv/bin/pytest tests/ -q 2>&1 | tail -3
```

Expected: `994 passed`.

#### Step 3: Audit for proprietary references

```bash
grep -ri "workshield\|work.shield" --include="*.py" --include="*.yaml" --include="*.md" --include="*.toml" .
```

Expected: no matches in production code (matches in `docs/superpowers/plans/` are command quotations and can be ignored).

#### Step 4: Commit and push

```bash
git add siphon/__init__.py
git commit -m "chore: bump version to 0.3.0 (Plans A/B/C complete)"
git push -u origin v3-plan-c-polish
```

---

## Verification

After all tasks:
1. `pytest tests/ -q` shows `994 passed`.
2. README has a Quickstart section with the 5-minute walkthrough at the top.
3. README has a Cookbook section with 3 real recipes.
4. Existing reference sections (Field Types, Upserts, etc.) are preserved.
5. Version is `0.3.0`.
6. Branch `v3-plan-c-polish` pushed to origin.
7. Random spot check: pick 3 errors from the audit, run `siphon` to trigger them, confirm the new messages are useful.

## Out of Scope

- **Bulk insert performance work** — the audit's "medium value" rating doesn't justify the architectural complexity. Defer to a future plan if real users complain about throughput.
- **API documentation** (e.g., docstrings rendered as docs site) — the README is the doc; no doc site for now.
- **Migration guide for v2 → v3 users** — no v2 users yet (it's pre-release).
- **Demo videos / screenshots** — text-only documentation.
- **Type stubs / `py.typed`** — Pydantic gives us type info already.

## Self-Review Notes

**Behavior preservation:** Error messages change content but not error TYPES. All `pytest.raises(ConfigError)` assertions continue to pass. Tests that match on specific message regex MAY need their regex loosened (`match="enum.*missing"` instead of an exact string). Document any such adjustments.

**Test count:** Stays at 994 throughout. No new tests, no deletions.

**README:** Quickstart goes at the top. Cookbook follows. Existing reference sections stay intact below — they're still valuable for users past the first 5 minutes.

**Placeholder scan:** None.

**Type consistency:** No type changes — pure documentation/messaging work.
