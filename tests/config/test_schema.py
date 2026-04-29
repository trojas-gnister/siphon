"""Tests for siphon/config/schema.py — Pydantic config schema models."""

import pytest
from pydantic import ValidationError

from siphon.config.schema import (
    BelongsToRelationship,
    CollectionConfig,
    DatabaseConfig,
    DeduplicationConfig,
    FieldConfig,
    FieldDBConfig,
    JunctionRelationship,
    PipelineConfig,
    PrimaryKeyConfig,
    SchemaConfig,
    SiphonConfig,
    SourceConfig,
    TableConfig,
    TransformFieldConfig,
    TransformFileConfig,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_dict(**overrides) -> dict:
    """Return a minimal valid SiphonConfig dict, with optional overrides applied at top level."""
    base = {
        "name": "test-pipeline",
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
                    "type": "string",
                    "source": "Company Name",
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
# SourceConfig
# ---------------------------------------------------------------------------


class TestSourceConfig:
    def test_spreadsheet_type(self):
        cfg = SourceConfig(type="spreadsheet")
        assert cfg.type == "spreadsheet"
        assert cfg.root is None
        assert cfg.encoding == "utf-8"
        assert cfg.force_list is None

    def test_xml_type_with_root(self):
        cfg = SourceConfig(type="xml", root="Records.Record", encoding="utf-16")
        assert cfg.type == "xml"
        assert cfg.root == "Records.Record"
        assert cfg.encoding == "utf-16"

    def test_xml_with_force_list(self):
        cfg = SourceConfig(type="xml", root="Data", force_list=["Item", "Note"])
        assert cfg.force_list == ["Item", "Note"]

    def test_json_type(self):
        cfg = SourceConfig(type="json", root="data.items")
        assert cfg.type == "json"
        assert cfg.root == "data.items"

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            SourceConfig(type="csv")

    def test_missing_type_rejected(self):
        with pytest.raises(ValidationError):
            SourceConfig()


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
# TransformFieldConfig
# ---------------------------------------------------------------------------


class TestTransformFieldConfig:
    def test_template_transform(self):
        cfg = TransformFieldConfig(type="template", template="Dear {first_name}")
        assert cfg.type == "template"
        assert cfg.template == "Dear {first_name}"

    def test_map_transform(self):
        cfg = TransformFieldConfig(
            type="map",
            values={"M": "Male", "F": "Female"},
            default="Unknown",
        )
        assert cfg.type == "map"
        assert cfg.values == {"M": "Male", "F": "Female"}
        assert cfg.default == "Unknown"

    def test_concat_transform(self):
        cfg = TransformFieldConfig(
            type="concat",
            fields=["first_name", "last_name"],
            separator=", ",
        )
        assert cfg.type == "concat"
        assert cfg.fields == ["first_name", "last_name"]
        assert cfg.separator == ", "

    def test_uuid_transform(self):
        cfg = TransformFieldConfig(type="uuid")
        assert cfg.type == "uuid"

    def test_now_transform(self):
        cfg = TransformFieldConfig(type="now", format="%Y-%m-%d")
        assert cfg.type == "now"
        assert cfg.format == "%Y-%m-%d"

    def test_custom_transform(self):
        cfg = TransformFieldConfig(
            type="custom",
            function="classify_severity",
            args=["description", "category"],
        )
        assert cfg.function == "classify_severity"
        assert cfg.args == ["description", "category"]

    def test_coalesce_with_fallback(self):
        cfg = TransformFieldConfig(
            type="coalesce",
            fields=["preferred_name", "first_name"],
            fallback=TransformFieldConfig(type="template", template="N/A"),
        )
        assert cfg.type == "coalesce"
        assert cfg.fallback is not None
        assert cfg.fallback.type == "template"

    def test_separator_default(self):
        cfg = TransformFieldConfig(type="concat")
        assert cfg.separator == " "

    def test_missing_type_rejected(self):
        with pytest.raises(ValidationError):
            TransformFieldConfig()


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
        assert field.source is None
        assert field.aliases is None
        assert field.transform is None
        assert field.value is None

    def test_field_with_source(self):
        field = FieldConfig(
            name="company_name",
            type="string",
            source="Company Name",
            db=FieldDBConfig(table="companies", column="name"),
        )
        assert field.source == "Company Name"

    def test_field_with_aliases(self):
        field = FieldConfig(
            name="company_name",
            type="string",
            source="CompanyName",
            aliases=["Company Name", "company_name", "Name"],
            db=FieldDBConfig(table="companies", column="name"),
        )
        assert field.aliases == ["Company Name", "company_name", "Name"]

    def test_field_with_transform(self):
        field = FieldConfig(
            name="full_name",
            type="string",
            transform=TransformFieldConfig(
                type="concat",
                fields=["first_name", "last_name"],
                separator=" ",
            ),
            db=FieldDBConfig(table="people", column="full_name"),
        )
        assert field.transform is not None
        assert field.transform.type == "concat"

    def test_field_with_constant_value(self):
        field = FieldConfig(
            name="status",
            type="string",
            value="active",
            db=FieldDBConfig(table="companies", column="status"),
        )
        assert field.value == "active"

    def test_field_with_bool_constant(self):
        field = FieldConfig(
            name="is_active",
            type="boolean",
            value=True,
            db=FieldDBConfig(table="companies", column="is_active"),
        )
        assert field.value is True

    def test_field_with_int_constant(self):
        field = FieldConfig(
            name="priority",
            type="integer",
            value=1,
            db=FieldDBConfig(table="items", column="priority"),
        )
        assert field.value == 1

    def test_field_type_optional(self):
        """type is now optional — fields that only use transform may not need it."""
        field = FieldConfig(
            name="record_id",
            transform=TransformFieldConfig(type="uuid"),
            db=FieldDBConfig(table="records", column="record_id"),
        )
        assert field.type is None

    def test_field_no_source_no_transform_no_value_allowed(self):
        """A field with no source, transform, or value is allowed (may be set elsewhere)."""
        field = FieldConfig(
            name="placeholder",
            db=FieldDBConfig(table="t", column="c"),
        )
        assert field.source is None
        assert field.transform is None
        assert field.value is None

    def test_all_optional_attrs(self):
        field = FieldConfig(
            name="state",
            type="enum",
            source="State",
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

    def test_missing_db_rejected(self):
        with pytest.raises(ValidationError):
            FieldConfig(name="x", type="string")

    def test_constraint_ordering_min_gt_max(self):
        with pytest.raises(ValidationError, match="min.*must be <= max"):
            FieldConfig(
                name="x",
                type="number",
                min=10.0,
                max=5.0,
                db=FieldDBConfig(table="t", column="c"),
            )

    def test_constraint_ordering_min_length_gt_max_length(self):
        with pytest.raises(ValidationError, match="min_length.*must be <= max_length"):
            FieldConfig(
                name="x",
                type="string",
                min_length=10,
                max_length=5,
                db=FieldDBConfig(table="t", column="c"),
            )


# ---------------------------------------------------------------------------
# CollectionConfig
# ---------------------------------------------------------------------------


class TestCollectionConfig:
    def test_valid_collection(self):
        coll = CollectionConfig(
            name="case_notes",
            source_path="CaseNotes.CaseNote",
            fields=[
                FieldConfig(
                    name="note_text",
                    type="string",
                    source="Text",
                    db=FieldDBConfig(table="case_notes", column="text"),
                ),
                FieldConfig(
                    name="note_date",
                    type="date",
                    source="Date",
                    db=FieldDBConfig(table="case_notes", column="created_at"),
                ),
            ],
        )
        assert coll.name == "case_notes"
        assert coll.source_path == "CaseNotes.CaseNote"
        assert len(coll.fields) == 2

    def test_missing_name_rejected(self):
        with pytest.raises(ValidationError):
            CollectionConfig(
                source_path="Items.Item",
                fields=[],
            )

    def test_missing_source_path_rejected(self):
        with pytest.raises(ValidationError):
            CollectionConfig(
                name="items",
                fields=[],
            )

    def test_collection_with_transform_field(self):
        coll = CollectionConfig(
            name="notes",
            source_path="Notes.Note",
            fields=[
                FieldConfig(
                    name="note_id",
                    transform=TransformFieldConfig(type="uuid"),
                    db=FieldDBConfig(table="notes", column="id"),
                ),
            ],
        )
        assert coll.fields[0].transform.type == "uuid"


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
# TransformFileConfig
# ---------------------------------------------------------------------------


class TestTransformFileConfig:
    def test_with_file_path(self):
        cfg = TransformFileConfig(file="transforms/custom.py")
        assert cfg.file == "transforms/custom.py"

    def test_default_file_none(self):
        cfg = TransformFileConfig()
        assert cfg.file is None


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
        assert schema.collections is None

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

    def test_with_collections(self):
        schema = SchemaConfig(
            fields=[
                FieldConfig(
                    name="company_name",
                    type="string",
                    db=FieldDBConfig(table="companies", column="name"),
                )
            ],
            collections=[
                CollectionConfig(
                    name="notes",
                    source_path="Notes.Note",
                    fields=[
                        FieldConfig(
                            name="note_text",
                            type="string",
                            source="Text",
                            db=FieldDBConfig(table="notes", column="text"),
                        )
                    ],
                )
            ],
            tables={
                "companies": TableConfig(
                    primary_key=PrimaryKeyConfig(column="id", type="auto_increment")
                ),
                "notes": TableConfig(
                    primary_key=PrimaryKeyConfig(column="id", type="auto_increment")
                ),
            },
        )
        assert len(schema.collections) == 1
        assert schema.collections[0].name == "notes"


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
    def test_valid_v2_config_with_source(self):
        """A v2 config with source (no llm) must be accepted."""
        cfg = SiphonConfig.model_validate(_minimal_dict())
        assert cfg.name == "test-pipeline"
        assert cfg.source.type == "spreadsheet"
        assert cfg.database.url == "sqlite+aiosqlite:///test.db"
        assert len(cfg.schema_.fields) == 1
        assert cfg.schema_.fields[0].name == "company_name"
        assert "companies" in cfg.schema_.tables

    def test_minimal_valid_config(self):
        cfg = SiphonConfig.model_validate(_minimal_dict())
        assert cfg.name == "test-pipeline"
        assert cfg.relationships == []
        assert cfg.pipeline.chunk_size == 25
        assert cfg.transforms is None
        assert cfg.variables is None

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

    def test_missing_source_rejected(self):
        data = _minimal_dict()
        del data["source"]
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

    def test_source_xml_with_root(self):
        data = _minimal_dict(source={"type": "xml", "root": "Records.Record"})
        cfg = SiphonConfig.model_validate(data)
        assert cfg.source.type == "xml"
        assert cfg.source.root == "Records.Record"

    def test_source_json_type(self):
        data = _minimal_dict(source={"type": "json", "root": "data.items"})
        cfg = SiphonConfig.model_validate(data)
        assert cfg.source.type == "json"

    def test_transforms_section(self):
        data = _minimal_dict(transforms={"file": "transforms/custom.py"})
        cfg = SiphonConfig.model_validate(data)
        assert cfg.transforms is not None
        assert cfg.transforms.file == "transforms/custom.py"

    def test_variables_section(self):
        data = _minimal_dict(variables={"org_id": 42, "prefix": "WS"})
        cfg = SiphonConfig.model_validate(data)
        assert cfg.variables == {"org_id": 42, "prefix": "WS"}

    def test_field_with_source_mapping(self):
        data = _minimal_dict()
        data["schema"]["fields"][0]["source"] = "Company Name"
        cfg = SiphonConfig.model_validate(data)
        assert cfg.schema_.fields[0].source == "Company Name"

    def test_field_with_value_constant(self):
        data = _minimal_dict()
        data["schema"]["fields"].append(
            {
                "name": "status",
                "type": "string",
                "value": "active",
                "db": {"table": "companies", "column": "status"},
            }
        )
        cfg = SiphonConfig.model_validate(data)
        status_field = next(f for f in cfg.schema_.fields if f.name == "status")
        assert status_field.value == "active"

    def test_field_with_transform(self):
        data = _minimal_dict()
        data["schema"]["fields"].append(
            {
                "name": "full_name",
                "type": "string",
                "transform": {
                    "type": "concat",
                    "fields": ["first_name", "last_name"],
                    "separator": " ",
                },
                "db": {"table": "companies", "column": "full_name"},
            }
        )
        cfg = SiphonConfig.model_validate(data)
        full_name = next(f for f in cfg.schema_.fields if f.name == "full_name")
        assert full_name.transform.type == "concat"

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

    def test_collection_unknown_table_caught(self):
        """A collection field referencing a table not in tables dict must raise."""
        data = _minimal_dict()
        data["schema"]["collections"] = [
            {
                "name": "notes",
                "source_path": "Notes.Note",
                "fields": [
                    {
                        "name": "note_text",
                        "type": "string",
                        "source": "Text",
                        "db": {"table": "ghost_table", "column": "text"},
                    }
                ],
            }
        ]
        with pytest.raises(ValidationError, match="unknown table"):
            SiphonConfig.model_validate(data)

    def test_belongs_to_field_from_collection_accepted(self):
        """A belongs_to relationship referencing a field defined in a collection."""
        data = _minimal_dict()
        data["schema"]["tables"]["notes"] = {
            "primary_key": {"column": "id", "type": "auto_increment"},
        }
        data["schema"]["collections"] = [
            {
                "name": "notes",
                "source_path": "Notes.Note",
                "fields": [
                    {
                        "name": "note_author",
                        "type": "string",
                        "source": "Author",
                        "db": {"table": "notes", "column": "author"},
                    }
                ],
            }
        ]
        data["relationships"] = [
            {
                "type": "belongs_to",
                "field": "note_author",
                "table": "notes",
                "references": "companies",
                "fk_column": "company_id",
                "resolve_by": "name",
            }
        ]
        cfg = SiphonConfig.model_validate(data)
        assert len(cfg.relationships) == 1

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
             "source": "State",
             "db": {"table": "addresses", "column": "state_code"}}
        )
        data["schema"]["fields"].append(
            {"name": "parent_entity", "type": "string",
             "source": "Parent",
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

    def test_collections_in_full_config(self):
        """Collections should work when validated as part of SiphonConfig."""
        data = _minimal_dict()
        data["schema"]["tables"]["notes"] = {
            "primary_key": {"column": "id", "type": "auto_increment"}
        }
        data["schema"]["collections"] = [
            {
                "name": "notes",
                "source_path": "Notes.Note",
                "fields": [
                    {
                        "name": "note_text",
                        "type": "string",
                        "source": "Text",
                        "db": {"table": "notes", "column": "text"},
                    }
                ],
            }
        ]
        cfg = SiphonConfig.model_validate(data)
        assert len(cfg.schema_.collections) == 1
        assert cfg.schema_.collections[0].name == "notes"


# ---------------------------------------------------------------------------
# OnConflictConfig
# ---------------------------------------------------------------------------


class TestOnConflictConfig:
    def test_default_action_is_error(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"])
        assert cfg.action == "error"
        assert cfg.update_columns == "all"

    def test_action_update(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"], action="update")
        assert cfg.action == "update"

    def test_action_skip(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"], action="skip")
        assert cfg.action == "skip"

    def test_invalid_action_rejected(self):
        from pydantic import ValidationError
        from siphon.config.schema import OnConflictConfig
        with pytest.raises(ValidationError):
            OnConflictConfig(key=["name"], action="merge")

    def test_composite_key(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name", "country_code"])
        assert cfg.key == ["name", "country_code"]

    def test_empty_key_rejected(self):
        from pydantic import ValidationError
        from siphon.config.schema import OnConflictConfig
        with pytest.raises(ValidationError):
            OnConflictConfig(key=[])

    def test_update_columns_specific_list(self):
        from siphon.config.schema import OnConflictConfig
        cfg = OnConflictConfig(key=["name"], action="update",
                               update_columns=["phone", "website"])
        assert cfg.update_columns == ["phone", "website"]

    def test_table_config_with_on_conflict(self):
        from siphon.config.schema import TableConfig, PrimaryKeyConfig, OnConflictConfig
        tc = TableConfig(
            primary_key=PrimaryKeyConfig(column="id", type="auto_increment"),
            on_conflict=OnConflictConfig(key=["name"], action="update"),
        )
        assert tc.on_conflict.action == "update"

    def test_table_config_without_on_conflict(self):
        from siphon.config.schema import TableConfig, PrimaryKeyConfig
        tc = TableConfig(
            primary_key=PrimaryKeyConfig(column="id", type="auto_increment"),
        )
        assert tc.on_conflict is None
