"""Tests for siphon/config/schema.py — Pydantic config schema models."""

import pytest
from pydantic import ValidationError

from siphon.config.schema import (
    BelongsToRelationship,
    DatabaseConfig,
    DeduplicationConfig,
    FieldConfig,
    FieldDBConfig,
    JunctionRelationship,
    LLMConfig,
    PipelineConfig,
    PrimaryKeyConfig,
    SchemaConfig,
    SiphonConfig,
    TableConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_dict(**overrides) -> dict:
    """Return a minimal valid SiphonConfig dict, with optional overrides applied at top level."""
    base = {
        "name": "test-pipeline",
        "llm": {
            "base_url": "http://localhost:11434/v1",
            "model": "llama3",
        },
        "database": {
            "url": "sqlite+aiosqlite:///test.db",
        },
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
        "pipeline": {
            "chunk_size": 25,
            "review": False,
            "log_level": "info",
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# LLMConfig
# ---------------------------------------------------------------------------


class TestLLMConfig:
    def test_required_fields(self):
        cfg = LLMConfig(base_url="http://localhost/v1", model="llama3")
        assert cfg.base_url == "http://localhost/v1"
        assert cfg.model == "llama3"
        assert cfg.api_key == ""
        assert cfg.extraction_hints is None

    def test_optional_fields(self):
        cfg = LLMConfig(
            base_url="http://localhost/v1",
            model="llama3",
            api_key="sk-abc",
            extraction_hints="Hint text.",
        )
        assert cfg.api_key == "sk-abc"
        assert cfg.extraction_hints == "Hint text."

    def test_missing_base_url_rejected(self):
        with pytest.raises(ValidationError):
            LLMConfig(model="llama3")

    def test_missing_model_rejected(self):
        with pytest.raises(ValidationError):
            LLMConfig(base_url="http://localhost/v1")


# ---------------------------------------------------------------------------
# DatabaseConfig
# ---------------------------------------------------------------------------


class TestDatabaseConfig:
    def test_valid(self):
        cfg = DatabaseConfig(url="postgresql+asyncpg://user:pw@localhost/db")
        assert cfg.url == "postgresql+asyncpg://user:pw@localhost/db"

    def test_missing_url_rejected(self):
        with pytest.raises(ValidationError):
            DatabaseConfig()


# ---------------------------------------------------------------------------
# FieldConfig
# ---------------------------------------------------------------------------


class TestFieldConfig:
    def test_minimal_field(self):
        field = FieldConfig(
            name="company_name",
            type="string",
            db=FieldDBConfig(table="companies", column="name"),
        )
        assert field.name == "company_name"
        assert field.required is False
        assert field.min_length is None

    def test_all_optional_attrs(self):
        field = FieldConfig(
            name="state",
            type="enum",
            db=FieldDBConfig(table="addresses", column="state_code"),
            preset="us_states",
            values=["CA", "TX"],
            min_length=2,
            max_length=50,
            min=0.0,
            max=100.0,
            pattern=r"^\w+$",
            format="%Y-%m-%d",
            case="upper",
            country_code="US",
        )
        assert field.preset == "us_states"
        assert field.country_code == "US"
        assert field.case == "upper"

    def test_missing_name_rejected(self):
        with pytest.raises(ValidationError):
            FieldConfig(type="string", db=FieldDBConfig(table="t", column="c"))

    def test_missing_type_rejected(self):
        with pytest.raises(ValidationError):
            FieldConfig(name="x", db=FieldDBConfig(table="t", column="c"))

    def test_missing_db_rejected(self):
        with pytest.raises(ValidationError):
            FieldConfig(name="x", type="string")


# ---------------------------------------------------------------------------
# PrimaryKeyConfig
# ---------------------------------------------------------------------------


class TestPrimaryKeyConfig:
    def test_auto_increment(self):
        pk = PrimaryKeyConfig(column="id", type="auto_increment")
        assert pk.type == "auto_increment"

    def test_uuid(self):
        pk = PrimaryKeyConfig(column="id", type="uuid")
        assert pk.type == "uuid"

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            PrimaryKeyConfig(column="id", type="serial")


# ---------------------------------------------------------------------------
# DeduplicationConfig
# ---------------------------------------------------------------------------


class TestDeduplicationConfig:
    def test_defaults(self):
        dedup = DeduplicationConfig(key=["company_name"])
        assert dedup.check_db is False
        assert dedup.match == "exact"

    def test_case_insensitive(self):
        dedup = DeduplicationConfig(key=["company_name"], match="case_insensitive")
        assert dedup.match == "case_insensitive"

    def test_invalid_match_rejected(self):
        with pytest.raises(ValidationError):
            DeduplicationConfig(key=["name"], match="fuzzy")

    def test_missing_key_rejected(self):
        with pytest.raises(ValidationError):
            DeduplicationConfig()


# ---------------------------------------------------------------------------
# BelongsToRelationship
# ---------------------------------------------------------------------------


class TestBelongsToRelationship:
    def test_valid(self):
        rel = BelongsToRelationship(
            type="belongs_to",
            field="parent_entity",
            table="companies",
            references="companies",
            fk_column="parent_id",
            resolve_by="name",
        )
        assert rel.type == "belongs_to"

    def test_missing_field_rejected(self):
        with pytest.raises(ValidationError):
            BelongsToRelationship(
                type="belongs_to",
                table="companies",
                references="companies",
                fk_column="parent_id",
                resolve_by="name",
            )


# ---------------------------------------------------------------------------
# JunctionRelationship
# ---------------------------------------------------------------------------


class TestJunctionRelationship:
    def test_valid(self):
        rel = JunctionRelationship(
            type="junction",
            link=["companies", "addresses"],
            through="company_addresses",
            columns={"companies": "company_id", "addresses": "address_id"},
        )
        assert rel.link == ["companies", "addresses"]
        assert rel.through == "company_addresses"

    def test_link_must_be_two_items(self):
        with pytest.raises(ValidationError, match="exactly 2"):
            JunctionRelationship(
                type="junction",
                link=["companies"],
                through="company_addresses",
                columns={"companies": "company_id"},
            )

    def test_link_three_items_rejected(self):
        with pytest.raises(ValidationError, match="exactly 2"):
            JunctionRelationship(
                type="junction",
                link=["a", "b", "c"],
                through="ab_c",
                columns={"a": "a_id", "b": "b_id", "c": "c_id"},
            )


# ---------------------------------------------------------------------------
# SchemaConfig
# ---------------------------------------------------------------------------


class TestSchemaConfig:
    def test_valid_schema(self):
        schema = SchemaConfig(
            fields=[
                FieldConfig(
                    name="company_name",
                    type="string",
                    db=FieldDBConfig(table="companies", column="name"),
                )
            ],
            tables={
                "companies": TableConfig(
                    primary_key=PrimaryKeyConfig(column="id", type="auto_increment")
                )
            },
        )
        assert len(schema.fields) == 1
        assert "companies" in schema.tables
        assert schema.deduplication is None

    def test_with_deduplication(self):
        schema = SchemaConfig(
            fields=[
                FieldConfig(
                    name="company_name",
                    type="string",
                    db=FieldDBConfig(table="companies", column="name"),
                )
            ],
            tables={
                "companies": TableConfig(
                    primary_key=PrimaryKeyConfig(column="id", type="auto_increment")
                )
            },
            deduplication=DeduplicationConfig(key=["company_name"]),
        )
        assert schema.deduplication is not None


# ---------------------------------------------------------------------------
# PipelineConfig
# ---------------------------------------------------------------------------


class TestPipelineConfig:
    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.chunk_size == 25
        assert cfg.review is False
        assert cfg.log_level == "info"
        assert cfg.log_dir is None

    def test_custom_values(self):
        cfg = PipelineConfig(chunk_size=100, review=True, log_level="debug", log_dir="./logs")
        assert cfg.chunk_size == 100
        assert cfg.review is True
        assert cfg.log_dir == "./logs"


# ---------------------------------------------------------------------------
# SiphonConfig — top-level integration tests
# ---------------------------------------------------------------------------


class TestSiphonConfig:
    def test_valid_config_from_conftest_fixture(self, sample_config_dict):
        """The shape defined in conftest.py must be accepted."""
        cfg = SiphonConfig.model_validate(sample_config_dict)
        assert cfg.name == "test_pipeline"
        assert cfg.llm.model == "gpt-4o-mini"
        assert cfg.database.url == "sqlite+aiosqlite:///test.db"
        assert len(cfg.schema_.fields) == 1
        assert cfg.schema_.fields[0].name == "company_name"
        assert "companies" in cfg.schema_.tables

    def test_minimal_valid_config(self):
        cfg = SiphonConfig.model_validate(_minimal_dict())
        assert cfg.name == "test-pipeline"
        assert cfg.relationships == []
        assert cfg.pipeline.chunk_size == 25

    def test_schema_alias_works(self):
        """The YAML key 'schema' must map correctly to schema_ attribute."""
        cfg = SiphonConfig.model_validate(_minimal_dict())
        assert hasattr(cfg, "schema_")
        assert isinstance(cfg.schema_, SchemaConfig)

    def test_missing_name_rejected(self):
        data = _minimal_dict()
        del data["name"]
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate(data)

    def test_missing_llm_rejected(self):
        data = _minimal_dict()
        del data["llm"]
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate(data)

    def test_missing_database_rejected(self):
        data = _minimal_dict()
        del data["database"]
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate(data)

    def test_missing_schema_rejected(self):
        data = _minimal_dict()
        del data["schema"]
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate(data)

    def test_unknown_field_table_reference_caught(self):
        """A field referencing a table not in tables dict must raise ValidationError."""
        data = _minimal_dict()
        data["schema"]["fields"].append(
            {
                "name": "city",
                "type": "string",
                "db": {"table": "nonexistent_table", "column": "city"},
            }
        )
        with pytest.raises(ValidationError, match="unknown table"):
            SiphonConfig.model_validate(data)

    def test_belongs_to_unknown_table_caught(self):
        """A belongs_to relationship referencing an unknown table must raise ValidationError."""
        data = _minimal_dict()
        data["schema"]["fields"].append(
            {"name": "parent_entity", "type": "string",
             "db": {"table": "companies", "column": "parent_name"}}
        )
        data["relationships"] = [
            {
                "type": "belongs_to",
                "field": "parent_entity",
                "table": "ghost_table",
                "references": "companies",
                "fk_column": "parent_id",
                "resolve_by": "name",
            }
        ]
        with pytest.raises(ValidationError, match="unknown table"):
            SiphonConfig.model_validate(data)

    def test_belongs_to_unknown_references_caught(self):
        """A belongs_to relationship with unknown 'references' table must raise."""
        data = _minimal_dict()
        data["schema"]["fields"].append(
            {"name": "parent_entity", "type": "string",
             "db": {"table": "companies", "column": "parent_name"}}
        )
        data["relationships"] = [
            {
                "type": "belongs_to",
                "field": "parent_entity",
                "table": "companies",
                "references": "ghost_table",
                "fk_column": "parent_id",
                "resolve_by": "name",
            }
        ]
        with pytest.raises(ValidationError, match="unknown table"):
            SiphonConfig.model_validate(data)

    def test_junction_unknown_link_table_caught(self):
        """A junction relationship linking an unknown table must raise ValidationError."""
        data = _minimal_dict()
        data["relationships"] = [
            {
                "type": "junction",
                "link": ["companies", "ghost_table"],
                "through": "company_ghost",
                "columns": {"companies": "company_id", "ghost_table": "ghost_id"},
            }
        ]
        with pytest.raises(ValidationError, match="unknown table"):
            SiphonConfig.model_validate(data)

    def test_junction_link_must_be_two_items(self):
        """A junction with only one link item must be rejected."""
        data = _minimal_dict()
        data["relationships"] = [
            {
                "type": "junction",
                "link": ["companies"],
                "through": "company_x",
                "columns": {"companies": "company_id"},
            }
        ]
        with pytest.raises(ValidationError, match="exactly 2"):
            SiphonConfig.model_validate(data)

    def test_pipeline_defaults_when_absent(self):
        """Omitting pipeline section should use defaults."""
        data = _minimal_dict()
        del data["pipeline"]
        cfg = SiphonConfig.model_validate(data)
        assert cfg.pipeline.chunk_size == 25
        assert cfg.pipeline.review is False
        assert cfg.pipeline.log_level == "info"

    def test_belongs_to_unknown_field_caught(self):
        """A belongs_to relationship referencing an unknown field must raise."""
        data = _minimal_dict()
        data["relationships"] = [
            {
                "type": "belongs_to",
                "field": "nonexistent_field",
                "table": "companies",
                "references": "companies",
                "fk_column": "parent_id",
                "resolve_by": "name",
            }
        ]
        with pytest.raises(ValidationError, match="unknown field"):
            SiphonConfig.model_validate(data)

    def test_full_config_with_relationships(self):
        """Full config including both relationship types should parse correctly."""
        data = _minimal_dict()
        # Add a second table so junction can reference it
        data["schema"]["tables"]["addresses"] = {
            "primary_key": {"column": "id", "type": "auto_increment"}
        }
        data["schema"]["fields"].append(
            {"name": "state", "type": "enum", "preset": "us_states",
             "db": {"table": "addresses", "column": "state_code"}}
        )
        data["schema"]["fields"].append(
            {"name": "parent_entity", "type": "string",
             "db": {"table": "companies", "column": "parent_name"}}
        )
        data["relationships"] = [
            {
                "type": "belongs_to",
                "field": "parent_entity",
                "table": "companies",
                "references": "companies",
                "fk_column": "parent_id",
                "resolve_by": "name",
            },
            {
                "type": "junction",
                "link": ["companies", "addresses"],
                "through": "company_addresses",
                "columns": {"companies": "company_id", "addresses": "address_id"},
            },
        ]
        cfg = SiphonConfig.model_validate(data)
        assert len(cfg.relationships) == 2
        assert isinstance(cfg.relationships[0], BelongsToRelationship)
        assert isinstance(cfg.relationships[1], JunctionRelationship)

    def test_discriminated_union_rejects_unknown_type(self):
        """An unknown relationship type must be rejected."""
        data = _minimal_dict()
        data["relationships"] = [
            {"type": "has_many", "field": "employees", "table": "companies"}
        ]
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate(data)

    def test_uuid_primary_key_accepted(self):
        data = _minimal_dict()
        data["schema"]["tables"]["companies"]["primary_key"]["type"] = "uuid"
        cfg = SiphonConfig.model_validate(data)
        assert cfg.schema_.tables["companies"].primary_key.type == "uuid"

    def test_deduplication_in_schema(self):
        data = _minimal_dict()
        data["schema"]["deduplication"] = {
            "key": ["company_name"],
            "check_db": True,
            "match": "case_insensitive",
        }
        cfg = SiphonConfig.model_validate(data)
        assert cfg.schema_.deduplication is not None
        assert cfg.schema_.deduplication.match == "case_insensitive"
