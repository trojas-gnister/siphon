"""Tests for dynamic ORM model generation from config."""

import pytest
from sqlalchemy import Integer, String, Float, Numeric, Date, DateTime, Boolean, inspect, text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.schema import DatabaseConfig, SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.models import ModelGenerator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(overrides: dict | None = None) -> SiphonConfig:
    """Build a minimal SiphonConfig dict, apply overrides, and validate."""
    base = {
        "name": "test_pipeline",
        "llm": {
            "base_url": "https://api.example.com/v1",
            "model": "gpt-4o-mini",
            "api_key": "sk-test",
        },
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
        "pipeline": {"chunk_size": 25, "log_level": "info"},
    }
    if overrides:
        _deep_merge(base, overrides)
    return SiphonConfig.model_validate(base)


def _deep_merge(base: dict, overrides: dict) -> dict:
    """Recursively merge overrides into base dict in place."""
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


# ---------------------------------------------------------------------------
# Single table model tests
# ---------------------------------------------------------------------------


class TestSingleTableModel:
    def test_model_has_correct_tablename(self):
        """Generated model has the correct __tablename__."""
        config = _make_config()
        gen = ModelGenerator(config)
        models = gen.generate()

        assert "companies" in models
        assert models["companies"].__tablename__ == "companies"

    def test_model_has_pk_and_data_columns(self):
        """Generated model has both PK column and data columns."""
        config = _make_config()
        gen = ModelGenerator(config)
        models = gen.generate()

        model = models["companies"]
        col_names = {c.name for c in model.__table__.columns}
        assert "id" in col_names
        assert "name" in col_names

    def test_auto_increment_pk_is_integer_primary_key(self):
        """Auto-increment PK is an Integer primary key."""
        config = _make_config()
        gen = ModelGenerator(config)
        models = gen.generate()

        pk_col = models["companies"].__table__.c.id
        assert pk_col.primary_key is True
        assert isinstance(pk_col.type, Integer)
        assert pk_col.autoincrement is True or pk_col.autoincrement == "auto"

    def test_uuid_pk_is_string36_primary_key(self):
        """UUID PK is a String(36) primary key."""
        config = _make_config({
            "schema": {
                "tables": {
                    "companies": {
                        "primary_key": {"column": "id", "type": "uuid"},
                    },
                },
            },
        })
        gen = ModelGenerator(config)
        models = gen.generate()

        pk_col = models["companies"].__table__.c.id
        assert pk_col.primary_key is True
        assert isinstance(pk_col.type, String)
        assert pk_col.type.length == 36


# ---------------------------------------------------------------------------
# SQL type mapping tests
# ---------------------------------------------------------------------------


class TestSQLTypeMappings:
    """Verify that field types produce the expected SQL column types."""

    @pytest.fixture
    def multi_type_config(self):
        """Config with several field types mapped to a single table."""
        return _make_config({
            "schema": {
                "fields": [
                    {"name": "company_name", "type": "string", "required": True,
                     "db": {"table": "companies", "column": "name"}},
                    {"name": "employee_count", "type": "integer",
                     "db": {"table": "companies", "column": "employee_count"}},
                    {"name": "rating", "type": "number",
                     "db": {"table": "companies", "column": "rating"}},
                    {"name": "revenue", "type": "currency",
                     "db": {"table": "companies", "column": "revenue"}},
                    {"name": "founded", "type": "date",
                     "db": {"table": "companies", "column": "founded"}},
                    {"name": "updated_at", "type": "datetime",
                     "db": {"table": "companies", "column": "updated_at"}},
                    {"name": "is_active", "type": "boolean",
                     "db": {"table": "companies", "column": "is_active"}},
                ],
            },
        })

    def test_string_maps_to_string(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.name
        assert isinstance(col.type, String)

    def test_integer_maps_to_integer(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.employee_count
        assert isinstance(col.type, Integer)

    def test_number_maps_to_float(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.rating
        assert isinstance(col.type, Float)

    def test_currency_maps_to_numeric(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.revenue
        assert isinstance(col.type, Numeric)

    def test_date_maps_to_date(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.founded
        assert isinstance(col.type, Date)

    def test_datetime_maps_to_datetime(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.updated_at
        assert isinstance(col.type, DateTime)

    def test_boolean_maps_to_boolean(self, multi_type_config):
        gen = ModelGenerator(multi_type_config)
        models = gen.generate()
        col = models["companies"].__table__.c.is_active
        assert isinstance(col.type, Boolean)


# ---------------------------------------------------------------------------
# Belongs-to relationship tests
# ---------------------------------------------------------------------------


class TestBelongsToRelationship:
    @pytest.fixture
    def belongs_to_config(self):
        """Config with a belongs_to FK from contacts -> companies."""
        return SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "company_name", "type": "string", "required": True,
                     "db": {"table": "companies", "column": "name"}},
                    {"name": "contact_name", "type": "string", "required": True,
                     "db": {"table": "contacts", "column": "name"}},
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

    def test_fk_column_added_to_child_table(self, belongs_to_config):
        """belongs_to adds a FK column to the child table."""
        gen = ModelGenerator(belongs_to_config)
        models = gen.generate()

        col_names = {c.name for c in models["contacts"].__table__.columns}
        assert "company_id" in col_names

    def test_fk_column_references_parent_pk(self, belongs_to_config):
        """FK column has a ForeignKey pointing to the parent table PK."""
        gen = ModelGenerator(belongs_to_config)
        models = gen.generate()

        fk_col = models["contacts"].__table__.c.company_id
        fk_refs = [fk.target_fullname for fk in fk_col.foreign_keys]
        assert "companies.id" in fk_refs

    def test_fk_column_type_matches_parent_pk_type(self, belongs_to_config):
        """FK column type matches the referenced PK type (Integer for auto_increment)."""
        gen = ModelGenerator(belongs_to_config)
        models = gen.generate()

        fk_col = models["contacts"].__table__.c.company_id
        assert isinstance(fk_col.type, Integer)

    def test_fk_column_is_nullable(self, belongs_to_config):
        """FK column is nullable by default."""
        gen = ModelGenerator(belongs_to_config)
        models = gen.generate()

        fk_col = models["contacts"].__table__.c.company_id
        assert fk_col.nullable is True

    def test_self_referential_belongs_to(self):
        """FK can reference the same table (self-referential)."""
        config = SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "emp_name", "type": "string", "required": True,
                     "db": {"table": "employees", "column": "name"}},
                ],
                "tables": {
                    "employees": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                },
            },
            "relationships": [
                {
                    "type": "belongs_to",
                    "field": "emp_name",
                    "table": "employees",
                    "references": "employees",
                    "fk_column": "manager_id",
                    "resolve_by": "name",
                },
            ],
        })
        gen = ModelGenerator(config)
        models = gen.generate()

        fk_col = models["employees"].__table__.c.manager_id
        fk_refs = [fk.target_fullname for fk in fk_col.foreign_keys]
        assert "employees.id" in fk_refs

    def test_belongs_to_with_uuid_pk(self):
        """FK type is String(36) when parent PK is uuid."""
        config = SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "org_name", "type": "string", "required": True,
                     "db": {"table": "orgs", "column": "name"}},
                    {"name": "project_name", "type": "string", "required": True,
                     "db": {"table": "projects", "column": "name"}},
                ],
                "tables": {
                    "orgs": {
                        "primary_key": {"column": "uid", "type": "uuid"},
                    },
                    "projects": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                },
            },
            "relationships": [
                {
                    "type": "belongs_to",
                    "field": "org_name",
                    "table": "projects",
                    "references": "orgs",
                    "fk_column": "org_uid",
                    "resolve_by": "name",
                },
            ],
        })
        gen = ModelGenerator(config)
        models = gen.generate()

        fk_col = models["projects"].__table__.c.org_uid
        assert isinstance(fk_col.type, String)
        assert fk_col.type.length == 36


# ---------------------------------------------------------------------------
# Junction table tests
# ---------------------------------------------------------------------------


class TestJunctionTable:
    @pytest.fixture
    def junction_config(self):
        """Config with a junction table between companies and categories."""
        return SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "company_name", "type": "string", "required": True,
                     "db": {"table": "companies", "column": "name"}},
                    {"name": "category_name", "type": "string", "required": True,
                     "db": {"table": "categories", "column": "name"}},
                ],
                "tables": {
                    "companies": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                    "categories": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                },
            },
            "relationships": [
                {
                    "type": "junction",
                    "link": ["companies", "categories"],
                    "through": "company_categories",
                    "columns": {
                        "companies": "company_id",
                        "categories": "category_id",
                    },
                },
            ],
        })

    def test_junction_model_created(self, junction_config):
        """Junction table model is created with correct tablename."""
        gen = ModelGenerator(junction_config)
        models = gen.generate()

        assert "company_categories" in models
        assert models["company_categories"].__tablename__ == "company_categories"

    def test_junction_has_composite_pk(self, junction_config):
        """Junction table has a composite primary key of two columns."""
        gen = ModelGenerator(junction_config)
        models = gen.generate()

        pk_cols = models["company_categories"].__table__.primary_key.columns
        pk_names = {c.name for c in pk_cols}
        assert pk_names == {"company_id", "category_id"}

    def test_junction_columns_are_foreign_keys(self, junction_config):
        """Both junction columns are foreign keys to the linked tables."""
        gen = ModelGenerator(junction_config)
        models = gen.generate()

        jt = models["company_categories"].__table__

        company_fks = [fk.target_fullname for fk in jt.c.company_id.foreign_keys]
        assert "companies.id" in company_fks

        category_fks = [fk.target_fullname for fk in jt.c.category_id.foreign_keys]
        assert "categories.id" in category_fks

    def test_junction_fk_types_match_referenced_pks(self, junction_config):
        """Junction FK column types match the referenced PK types."""
        gen = ModelGenerator(junction_config)
        models = gen.generate()

        jt = models["company_categories"].__table__
        assert isinstance(jt.c.company_id.type, Integer)
        assert isinstance(jt.c.category_id.type, Integer)


# ---------------------------------------------------------------------------
# Multiple tables / field grouping tests
# ---------------------------------------------------------------------------


class TestMultipleTables:
    def test_fields_grouped_to_correct_tables(self):
        """Fields are grouped to their correct tables based on db.table."""
        config = SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "company_name", "type": "string", "required": True,
                     "db": {"table": "companies", "column": "name"}},
                    {"name": "company_phone", "type": "phone",
                     "db": {"table": "companies", "column": "phone"}},
                    {"name": "contact_name", "type": "string", "required": True,
                     "db": {"table": "contacts", "column": "name"}},
                    {"name": "contact_email", "type": "email",
                     "db": {"table": "contacts", "column": "email"}},
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
        })
        gen = ModelGenerator(config)
        models = gen.generate()

        company_cols = {c.name for c in models["companies"].__table__.columns}
        contact_cols = {c.name for c in models["contacts"].__table__.columns}

        assert company_cols == {"id", "name", "phone"}
        assert contact_cols == {"id", "name", "email"}

    def test_table_with_no_fields_still_has_pk(self):
        """A table with no fields still gets a PK column."""
        config = SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "company_name", "type": "string", "required": True,
                     "db": {"table": "companies", "column": "name"}},
                ],
                "tables": {
                    "companies": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                    "lookup_table": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                },
            },
        })
        gen = ModelGenerator(config)
        models = gen.generate()

        assert "lookup_table" in models
        col_names = {c.name for c in models["lookup_table"].__table__.columns}
        assert "id" in col_names


# ---------------------------------------------------------------------------
# Round-trip: create tables in a real (in-memory) database
# ---------------------------------------------------------------------------


class TestRoundTrip:
    async def test_create_tables_via_engine(self):
        """Generated models can actually create tables in an in-memory SQLite DB."""
        config = SiphonConfig.model_validate({
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com/v1",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite://"},
            "schema": {
                "fields": [
                    {"name": "company_name", "type": "string", "required": True,
                     "db": {"table": "companies", "column": "name"}},
                    {"name": "contact_name", "type": "string", "required": True,
                     "db": {"table": "contacts", "column": "name"}},
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
                {
                    "type": "junction",
                    "link": ["companies", "contacts"],
                    "through": "company_contacts",
                    "columns": {
                        "companies": "company_id",
                        "contacts": "contact_id",
                    },
                },
            ],
        })

        gen = ModelGenerator(config)
        models = gen.generate()
        engine = DatabaseEngine(config.database)

        try:
            await engine.create_tables(gen.base)

            # Verify tables were created
            async with engine.engine.connect() as conn:
                def _get_tables(sync_conn):
                    return inspect(sync_conn).get_table_names()

                tables = await conn.run_sync(_get_tables)

            assert "companies" in tables
            assert "contacts" in tables
            assert "company_contacts" in tables
        finally:
            await engine.dispose()

    async def test_insert_and_query_generated_model(self):
        """Can insert and query data through a dynamically generated model."""
        config = _make_config()
        gen = ModelGenerator(config)
        models = gen.generate()
        engine = DatabaseEngine(config.database)

        try:
            await engine.create_tables(gen.base)

            Company = models["companies"]
            async with engine.session() as session:
                session.add(Company(name="Acme Corp"))
                await session.commit()

            async with engine.session() as session:
                result = await session.execute(
                    text("SELECT name FROM companies WHERE name = 'Acme Corp'")
                )
                name = result.scalar()

            assert name == "Acme Corp"
        finally:
            await engine.dispose()


# ---------------------------------------------------------------------------
# Generator properties
# ---------------------------------------------------------------------------


class TestGeneratorProperties:
    def test_base_is_declarative_base(self):
        """The base property returns a DeclarativeBase subclass."""
        config = _make_config()
        gen = ModelGenerator(config)
        from sqlalchemy.orm import DeclarativeBase
        assert issubclass(gen.base, DeclarativeBase)

    def test_models_empty_before_generate(self):
        """The models dict is empty before generate() is called."""
        config = _make_config()
        gen = ModelGenerator(config)
        assert gen.models == {}

    def test_generate_returns_same_as_models_property(self):
        """generate() return value and .models property are the same dict."""
        config = _make_config()
        gen = ModelGenerator(config)
        result = gen.generate()
        assert result is gen.models

    def test_each_generator_gets_fresh_base(self):
        """Each ModelGenerator instance gets its own DeclarativeBase."""
        config = _make_config()
        gen1 = ModelGenerator(config)
        gen2 = ModelGenerator(config)
        assert gen1.base is not gen2.base
