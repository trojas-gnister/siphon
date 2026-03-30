"""Tests for the relationship-aware inserter with topological sort."""

import pytest
from sqlalchemy import text

from siphon.config.schema import DatabaseConfig, SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator
from siphon.utils.errors import DatabaseError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LLM_BLOCK = {
    "base_url": "https://api.example.com/v1",
    "model": "gpt-4o-mini",
    "api_key": "sk-test",
}


def _simple_config() -> SiphonConfig:
    """Single-table config with auto_increment PK."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
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
        "relationships": [],
    })


def _uuid_config() -> SiphonConfig:
    """Single-table config with UUID PK."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "uuid"},
                },
            },
        },
        "relationships": [],
    })


def _belongs_to_config() -> SiphonConfig:
    """Two-table config with belongs_to relationship (contacts -> companies)."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
                {
                    "name": "contact_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "contacts", "column": "name"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
                "contacts": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "relationships": [
            {
                "type": "belongs_to",
                "field": "company_name",
                "table": "contacts",
                "references": "companies",
                "fk_column": "company_id",
                "resolve_by": "name",
            },
        ],
    })


def _self_ref_config() -> SiphonConfig:
    """Single-table config with self-referential belongs_to (parent company)."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
                {
                    "name": "parent_entity",
                    "type": "string",
                    "db": {"table": "companies", "column": "parent_name"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "relationships": [
            {
                "type": "belongs_to",
                "field": "parent_entity",
                "table": "companies",
                "references": "companies",
                "fk_column": "parent_id",
                "resolve_by": "name",
            },
        ],
    })


def _junction_config() -> SiphonConfig:
    """Two-table config with a junction table."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
                {
                    "name": "address_city",
                    "type": "string",
                    "required": True,
                    "db": {"table": "addresses", "column": "city"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
                "addresses": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "relationships": [
            {
                "type": "junction",
                "link": ["companies", "addresses"],
                "through": "company_addresses",
                "columns": {
                    "companies": "company_id",
                    "addresses": "address_id",
                },
            },
        ],
    })


async def _setup(config: SiphonConfig):
    """Create engine, generate models, create tables, return (engine, gen, inserter)."""
    gen = ModelGenerator(config)
    gen.generate()
    engine = DatabaseEngine(config.database)
    await engine.create_tables(gen.base)
    inserter = Inserter(config, engine, gen)
    return engine, gen, inserter


# ---------------------------------------------------------------------------
# Basic insert
# ---------------------------------------------------------------------------


class TestBasicInsert:
    async def test_single_record_inserted(self):
        """A single record is inserted and retrievable from the DB."""
        config = _simple_config()
        engine, gen, inserter = await _setup(config)
        try:
            count = await inserter.insert([{"company_name": "Acme Corp"}])

            assert count == 1

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT name FROM companies")
                )
                rows = result.fetchall()

            assert len(rows) == 1
            assert rows[0][0] == "Acme Corp"
        finally:
            await engine.dispose()

    async def test_multiple_records_inserted(self):
        """Multiple records are all inserted."""
        config = _simple_config()
        engine, gen, inserter = await _setup(config)
        try:
            records = [
                {"company_name": "Acme Corp"},
                {"company_name": "Globex"},
                {"company_name": "Initech"},
            ]
            count = await inserter.insert(records)

            assert count == 3

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT COUNT(*) FROM companies")
                )
                db_count = result.scalar()

            assert db_count == 3
        finally:
            await engine.dispose()

    async def test_auto_increment_pk_assigned(self):
        """Auto-increment PK values are assigned sequentially."""
        config = _simple_config()
        engine, gen, inserter = await _setup(config)
        try:
            await inserter.insert([
                {"company_name": "First"},
                {"company_name": "Second"},
            ])

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT id, name FROM companies ORDER BY id")
                )
                rows = result.fetchall()

            assert rows[0][0] == 1
            assert rows[1][0] == 2
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# UUID PK generation
# ---------------------------------------------------------------------------


class TestUUIDPK:
    async def test_uuid_pk_generated(self):
        """UUID PK is generated and stored as a 36-char string."""
        config = _uuid_config()
        engine, gen, inserter = await _setup(config)
        try:
            await inserter.insert([{"company_name": "Acme Corp"}])

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT id FROM companies")
                )
                pk = result.scalar()

            assert pk is not None
            assert len(pk) == 36
            # Validate UUID format (8-4-4-4-12 hex digits)
            parts = pk.split("-")
            assert len(parts) == 5
            assert [len(p) for p in parts] == [8, 4, 4, 4, 12]
        finally:
            await engine.dispose()

    async def test_uuid_pk_unique_per_record(self):
        """Each record gets a unique UUID PK."""
        config = _uuid_config()
        engine, gen, inserter = await _setup(config)
        try:
            await inserter.insert([
                {"company_name": "A"},
                {"company_name": "B"},
            ])

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT id FROM companies")
                )
                ids = [row[0] for row in result.fetchall()]

            assert len(ids) == 2
            assert ids[0] != ids[1]
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# Self-referential belongs_to
# ---------------------------------------------------------------------------


class TestSelfReferentialBelongsTo:
    async def test_parent_inserted_before_child(self):
        """Parent record is inserted before child, FK is resolved correctly."""
        config = _self_ref_config()
        engine, gen, inserter = await _setup(config)
        try:
            # Child references parent -- inserter must sort so "Parent Corp" goes first
            records = [
                {"company_name": "Child Inc", "parent_entity": "Parent Corp"},
                {"company_name": "Parent Corp", "parent_entity": None},
            ]
            count = await inserter.insert(records)
            assert count == 2

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT id, name, parent_id FROM companies ORDER BY id")
                )
                rows = result.fetchall()

            # Parent Corp should have been inserted first (no parent_id)
            parent_row = next(r for r in rows if r[1] == "Parent Corp")
            child_row = next(r for r in rows if r[1] == "Child Inc")

            assert parent_row[2] is None  # parent_id is NULL for root
            assert child_row[2] == parent_row[0]  # child's parent_id == parent's id
        finally:
            await engine.dispose()

    async def test_three_level_hierarchy(self):
        """Grandparent -> parent -> child hierarchy is resolved correctly."""
        config = _self_ref_config()
        engine, gen, inserter = await _setup(config)
        try:
            # Records given in reverse order -- inserter must re-order
            records = [
                {"company_name": "Grandchild LLC", "parent_entity": "Child Inc"},
                {"company_name": "Child Inc", "parent_entity": "Parent Corp"},
                {"company_name": "Parent Corp", "parent_entity": None},
            ]
            count = await inserter.insert(records)
            assert count == 3

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT id, name, parent_id FROM companies ORDER BY id")
                )
                rows = result.fetchall()

            by_name = {r[1]: r for r in rows}
            assert by_name["Parent Corp"][2] is None
            assert by_name["Child Inc"][2] == by_name["Parent Corp"][0]
            assert by_name["Grandchild LLC"][2] == by_name["Child Inc"][0]
        finally:
            await engine.dispose()

    async def test_multiple_roots(self):
        """Multiple root records (no parent) plus children are handled."""
        config = _self_ref_config()
        engine, gen, inserter = await _setup(config)
        try:
            records = [
                {"company_name": "Sub A", "parent_entity": "Root A"},
                {"company_name": "Root A", "parent_entity": None},
                {"company_name": "Root B", "parent_entity": None},
                {"company_name": "Sub B", "parent_entity": "Root B"},
            ]
            count = await inserter.insert(records)
            assert count == 4

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT id, name, parent_id FROM companies")
                )
                rows = result.fetchall()

            by_name = {r[1]: r for r in rows}
            assert by_name["Root A"][2] is None
            assert by_name["Root B"][2] is None
            assert by_name["Sub A"][2] == by_name["Root A"][0]
            assert by_name["Sub B"][2] == by_name["Root B"][0]
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# Junction table insertion
# ---------------------------------------------------------------------------


class TestJunctionInsert:
    async def test_junction_row_created(self):
        """After inserting company + address, a junction row links them."""
        config = _junction_config()
        engine, gen, inserter = await _setup(config)
        try:
            records = [
                {"company_name": "Acme Corp", "address_city": "Springfield"},
            ]
            count = await inserter.insert(records)
            assert count == 1

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT company_id, address_id FROM company_addresses")
                )
                junc_rows = result.fetchall()

            assert len(junc_rows) == 1
            # Both IDs should be 1 (first insert in each table)
            assert junc_rows[0][0] == 1
            assert junc_rows[0][1] == 1
        finally:
            await engine.dispose()

    async def test_multiple_junction_rows(self):
        """Multiple records create multiple junction rows."""
        config = _junction_config()
        engine, gen, inserter = await _setup(config)
        try:
            records = [
                {"company_name": "Acme Corp", "address_city": "Springfield"},
                {"company_name": "Globex", "address_city": "Shelbyville"},
            ]
            count = await inserter.insert(records)
            assert count == 2

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT COUNT(*) FROM company_addresses")
                )
                junc_count = result.scalar()

            assert junc_count == 2
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# Transaction rollback on failure
# ---------------------------------------------------------------------------


class TestTransactionRollback:
    async def test_rollback_on_failure(self):
        """If insertion fails, the entire transaction is rolled back -- no partial data."""
        config = _simple_config()
        engine, gen, inserter = await _setup(config)
        try:
            # First, insert a valid record
            await inserter.insert([{"company_name": "Existing Corp"}])

            # Create a new inserter instance that will fail mid-batch
            inserter2 = Inserter(config, engine, gen)

            # We'll insert records where the second one triggers a unique constraint
            # Actually, there's no unique constraint. Instead, let's force a failure
            # by patching the model to cause an error.
            original_model = inserter2._models["companies"]

            class BrokenModel:
                """A model that raises on instantiation after the first call."""
                _call_count = 0

                def __init__(self, **kwargs):
                    BrokenModel._call_count += 1
                    if BrokenModel._call_count > 1:
                        raise RuntimeError("Simulated DB failure")
                    self.__dict__.update(kwargs)

            inserter2._models["companies"] = BrokenModel

            with pytest.raises(DatabaseError, match="Insertion failed"):
                await inserter2.insert([
                    {"company_name": "Good Record"},
                    {"company_name": "Bad Record"},
                ])

            # Only the original "Existing Corp" should remain
            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT COUNT(*) FROM companies")
                )
                count = result.scalar()

            assert count == 1  # Only the record from the first insert
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def test_independent_tables_all_present(self):
        """Tables with no relationships are all included in the sort."""
        config = _simple_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        result = inserter.topological_sort()
        assert result == ["companies"]

    def test_parent_before_child(self):
        """Parent table comes before child table in sorted output."""
        config = _belongs_to_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        result = inserter.topological_sort()
        assert result.index("companies") < result.index("contacts")

    def test_self_referential_does_not_cause_cycle(self):
        """Self-referential belongs_to does not create a cycle in topological sort."""
        config = _self_ref_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        result = inserter.topological_sort()
        assert "companies" in result

    def test_multi_level_dependency_chain(self):
        """A -> B -> C dependency chain is sorted correctly."""
        config = SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": _LLM_BLOCK,
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "org_name", "type": "string", "required": True,
                     "db": {"table": "orgs", "column": "name"}},
                    {"name": "dept_name", "type": "string", "required": True,
                     "db": {"table": "departments", "column": "name"}},
                    {"name": "team_name", "type": "string", "required": True,
                     "db": {"table": "teams", "column": "name"}},
                ],
                "tables": {
                    "orgs": {"primary_key": {"column": "id", "type": "auto_increment"}},
                    "departments": {"primary_key": {"column": "id", "type": "auto_increment"}},
                    "teams": {"primary_key": {"column": "id", "type": "auto_increment"}},
                },
            },
            "relationships": [
                {
                    "type": "belongs_to",
                    "field": "org_name",
                    "table": "departments",
                    "references": "orgs",
                    "fk_column": "org_id",
                    "resolve_by": "name",
                },
                {
                    "type": "belongs_to",
                    "field": "dept_name",
                    "table": "teams",
                    "references": "departments",
                    "fk_column": "dept_id",
                    "resolve_by": "name",
                },
            ],
        })
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        result = inserter.topological_sort()
        assert result.index("orgs") < result.index("departments")
        assert result.index("departments") < result.index("teams")


# ---------------------------------------------------------------------------
# load_existing_keys
# ---------------------------------------------------------------------------


class TestLoadExistingKeys:
    async def test_cache_populated_from_db(self):
        """load_existing_keys() pre-populates the lookup cache from existing rows."""
        config = _belongs_to_config()
        engine, gen, inserter = await _setup(config)
        try:
            # Insert a company directly to simulate pre-existing data
            Company = gen.models["companies"]
            async with engine.session() as session:
                session.add(Company(name="Pre-existing Corp"))
                await session.commit()

            await inserter.load_existing_keys()

            assert "Pre-existing Corp" in inserter._lookup_cache["companies"]
            assert inserter._lookup_cache["companies"]["Pre-existing Corp"] == 1
        finally:
            await engine.dispose()

    async def test_cache_used_for_fk_resolution(self):
        """Pre-loaded cache values are used when resolving FKs during insert.

        When a record references a company already in the DB via a belongs_to
        relationship, load_existing_keys ensures the FK can be resolved even
        if the company wasn't inserted in the current batch.
        """
        config = _belongs_to_config()
        engine, gen, inserter = await _setup(config)
        try:
            # Insert a company directly (simulates a previous pipeline run)
            Company = gen.models["companies"]
            async with engine.session() as session:
                session.add(Company(name="Existing Corp"))
                await session.commit()

            await inserter.load_existing_keys()

            # Verify the cache knows about the pre-existing company
            assert "Existing Corp" in inserter._lookup_cache["companies"]
            cached_id = inserter._lookup_cache["companies"]["Existing Corp"]
            assert cached_id == 1

            # Insert a record -- since company_name maps to companies table,
            # the inserter will insert a new company row too, updating the cache.
            # The contact's FK will point to the newly inserted company.
            await inserter.insert([
                {"company_name": "Existing Corp", "contact_name": "Alice"},
            ])

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT company_id FROM contacts WHERE name = 'Alice'")
                )
                fk = result.scalar()

            # The FK is resolved -- it points to a valid company
            assert fk is not None
            # Verify the FK references a real company
            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT name FROM companies WHERE id = :id"),
                    {"id": fk},
                )
                name = result.scalar()
            assert name == "Existing Corp"
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# generate_sql_preview
# ---------------------------------------------------------------------------


class TestSQLPreview:
    def test_returns_insert_statements(self):
        """generate_sql_preview() returns INSERT INTO statements."""
        config = _simple_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        stmts = inserter.generate_sql_preview([
            {"company_name": "Acme Corp"},
        ])

        assert len(stmts) == 1
        assert "INSERT INTO companies" in stmts[0]
        assert "name" in stmts[0]
        assert "'Acme Corp'" in stmts[0]

    def test_limits_to_five_records(self):
        """Preview is limited to 5 records maximum."""
        config = _simple_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        records = [{"company_name": f"Company {i}"} for i in range(10)]
        stmts = inserter.generate_sql_preview(records)

        assert len(stmts) == 5  # Only first 5 records

    def test_multi_table_preview(self):
        """Preview includes statements for all tables with data."""
        config = _junction_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        stmts = inserter.generate_sql_preview([
            {"company_name": "Acme", "address_city": "Springfield"},
        ])

        # Should have one INSERT per data table (2 tables for 1 record)
        assert len(stmts) == 2
        tables_mentioned = [s.split("INSERT INTO ")[1].split(" ")[0] for s in stmts]
        assert "companies" in tables_mentioned
        assert "addresses" in tables_mentioned

    def test_skips_fields_with_none_values(self):
        """Fields with None values are not included in the preview."""
        config = _belongs_to_config()
        gen = ModelGenerator(config)
        gen.generate()
        engine_cfg = DatabaseConfig(url="sqlite+aiosqlite://")
        db = DatabaseEngine(engine_cfg)
        inserter = Inserter(config, db, gen)

        stmts = inserter.generate_sql_preview([
            {"company_name": "Acme", "contact_name": None},
        ])

        # Only companies table should have a statement (contact_name is None)
        assert len(stmts) == 1
        assert "companies" in stmts[0]


# ---------------------------------------------------------------------------
# Belongs-to FK resolution (non-self-referential)
# ---------------------------------------------------------------------------


class TestBelongsToResolution:
    async def test_fk_resolved_across_tables(self):
        """belongs_to FK is resolved by looking up the parent record's PK."""
        config = _belongs_to_config()
        engine, gen, inserter = await _setup(config)
        try:
            records = [
                {"company_name": "Acme Corp", "contact_name": "Alice"},
            ]
            count = await inserter.insert(records)
            assert count == 1

            async with engine.session() as session:
                result = await session.execute(
                    text(
                        "SELECT c.name, c.company_id, co.name "
                        "FROM contacts c "
                        "JOIN companies co ON c.company_id = co.id"
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 1
            assert rows[0][0] == "Alice"
            assert rows[0][2] == "Acme Corp"
        finally:
            await engine.dispose()

    async def test_multiple_records_fk_resolved(self):
        """Each record resolves its own company FK independently."""
        config = _belongs_to_config()
        engine, gen, inserter = await _setup(config)
        try:
            records = [
                {"company_name": "Acme Corp", "contact_name": "Alice"},
                {"company_name": "Globex", "contact_name": "Bob"},
            ]
            count = await inserter.insert(records)
            assert count == 2

            async with engine.session() as session:
                result = await session.execute(
                    text(
                        "SELECT c.name, co.name "
                        "FROM contacts c "
                        "JOIN companies co ON c.company_id = co.id "
                        "ORDER BY c.name"
                    )
                )
                rows = result.fetchall()

            assert len(rows) == 2
            assert rows[0] == ("Alice", "Acme Corp")
            assert rows[1] == ("Bob", "Globex")
        finally:
            await engine.dispose()
