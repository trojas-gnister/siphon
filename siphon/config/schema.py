"""Pydantic models for the Siphon YAML configuration schema."""

from __future__ import annotations

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator


# ---------------------------------------------------------------------------
# Sub-models
# ---------------------------------------------------------------------------


class LLMConfig(BaseModel):
    """Configuration for the LLM provider."""

    model_config = ConfigDict(populate_by_name=True)

    base_url: str
    model: str
    api_key: str = ""
    extraction_hints: str | None = None


class DatabaseConfig(BaseModel):
    """Database connection configuration."""

    model_config = ConfigDict(populate_by_name=True)

    url: str


class FieldDBConfig(BaseModel):
    """Database mapping for a single field."""

    model_config = ConfigDict(populate_by_name=True)

    table: str
    column: str


FieldType = Literal[
    "string", "integer", "number", "currency",
    "phone", "url", "email", "date", "datetime",
    "enum", "boolean", "regex", "subdivision", "country",
]


class FieldConfig(BaseModel):
    """Definition of a single input field."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    type: FieldType
    db: FieldDBConfig
    required: bool = False

    # String constraints
    min_length: int | None = None
    max_length: int | None = None

    # Numeric constraints
    min: float | None = None
    max: float | None = None

    # Enum / preset
    values: list[str] | None = None
    preset: str | None = None

    # String / regex
    pattern: str | None = None

    # Date / datetime
    format: str | None = None

    # Case normalisation
    case: str | None = None

    # Subdivision type (e.g. us_states) — specifies which country's subdivisions to use
    country_code: str | None = None

    @model_validator(mode="after")
    def validate_constraint_ordering(self) -> "FieldConfig":
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(
                f"field '{self.name}': min ({self.min}) must be <= max ({self.max})"
            )
        if self.min_length is not None and self.max_length is not None and self.min_length > self.max_length:
            raise ValueError(
                f"field '{self.name}': min_length ({self.min_length}) must be <= max_length ({self.max_length})"
            )
        return self


class PrimaryKeyConfig(BaseModel):
    """Primary key definition for a table."""

    model_config = ConfigDict(populate_by_name=True)

    column: str
    type: Literal["auto_increment", "uuid"]


class TableConfig(BaseModel):
    """Configuration for a single database table."""

    model_config = ConfigDict(populate_by_name=True)

    primary_key: PrimaryKeyConfig


class DeduplicationConfig(BaseModel):
    """Deduplication strategy for the pipeline."""

    model_config = ConfigDict(populate_by_name=True)

    key: list[str]
    check_db: bool = False
    match: Literal["exact", "case_insensitive"] = "exact"


# ---------------------------------------------------------------------------
# Relationships (discriminated union on `type`)
# ---------------------------------------------------------------------------


class BelongsToRelationship(BaseModel):
    """A belongs-to (foreign key) relationship."""

    model_config = ConfigDict(populate_by_name=True)

    type: Literal["belongs_to"]
    field: str
    table: str
    references: str
    fk_column: str
    resolve_by: str


class JunctionRelationship(BaseModel):
    """A many-to-many junction table relationship."""

    model_config = ConfigDict(populate_by_name=True)

    type: Literal["junction"]
    link: list[str]
    through: str
    columns: dict[str, str]

    @model_validator(mode="after")
    def link_must_have_exactly_two_items(self) -> "JunctionRelationship":
        if len(self.link) != 2:
            raise ValueError(
                f"junction 'link' must contain exactly 2 table names, got {len(self.link)}"
            )
        return self


Relationship = Annotated[
    Union[BelongsToRelationship, JunctionRelationship],
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Schema section
# ---------------------------------------------------------------------------


class SchemaConfig(BaseModel):
    """The 'schema' section of the config file."""

    model_config = ConfigDict(populate_by_name=True)

    fields: list[FieldConfig]
    tables: dict[str, TableConfig]
    deduplication: DeduplicationConfig | None = None


# ---------------------------------------------------------------------------
# Pipeline section
# ---------------------------------------------------------------------------


class PipelineConfig(BaseModel):
    """Runtime pipeline options."""

    model_config = ConfigDict(populate_by_name=True)

    chunk_size: int = 25
    review: bool = False
    log_level: Literal["debug", "info", "warning", "error"] = "info"
    log_dir: str | None = None


# ---------------------------------------------------------------------------
# Top-level config
# ---------------------------------------------------------------------------


class SiphonConfig(BaseModel):
    """Root configuration model for a Siphon pipeline."""

    model_config = ConfigDict(populate_by_name=True)

    name: str
    llm: LLMConfig
    database: DatabaseConfig

    # 'schema' is a Python built-in — alias maps the YAML key to schema_
    schema_: SchemaConfig = Field(alias="schema")

    relationships: list[Relationship] = []
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)

    @model_validator(mode="after")
    def cross_validate_references(self) -> "SiphonConfig":
        """Ensure every field's db.table and relationship tables exist in schema.tables."""
        known_tables = set(self.schema_.tables.keys())
        known_field_names = {f.name for f in self.schema_.fields}

        # Validate field table references
        for field in self.schema_.fields:
            if field.db.table not in known_tables:
                raise ValueError(
                    f"field '{field.name}' references unknown table '{field.db.table}'; "
                    f"known tables: {sorted(known_tables)}"
                )

        # Validate relationship table references
        for rel in self.relationships:
            if isinstance(rel, BelongsToRelationship):
                if rel.field not in known_field_names:
                    raise ValueError(
                        f"belongs_to relationship fk_column='{rel.fk_column}' "
                        f"references unknown field '{rel.field}'; "
                        f"known fields: {sorted(known_field_names)}"
                    )
                for ref_table in (rel.table, rel.references):
                    if ref_table not in known_tables:
                        raise ValueError(
                            f"belongs_to relationship field='{rel.field}' references "
                            f"unknown table '{ref_table}'; known tables: {sorted(known_tables)}"
                        )
            elif isinstance(rel, JunctionRelationship):
                for link_table in rel.link:
                    if link_table not in known_tables:
                        raise ValueError(
                            f"junction relationship through='{rel.through}' links unknown "
                            f"table '{link_table}'; known tables: {sorted(known_tables)}"
                        )

        return self
