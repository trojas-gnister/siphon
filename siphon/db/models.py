"""Dynamic ORM model generation from Siphon configuration."""

from __future__ import annotations

import uuid

from sqlalchemy import Column, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase

from siphon.config.schema import (
    BelongsToRelationship,
    JunctionRelationship,
    SiphonConfig,
)
from siphon.config.types import get_sql_type


class ModelGenerator:
    """Generates SQLAlchemy ORM model classes dynamically from a SiphonConfig.

    Usage::

        gen = ModelGenerator(config)
        models = gen.generate()
        # models is a dict of table_name -> ORM class
        # gen.base is the DeclarativeBase for create_tables()
    """

    def __init__(self, config: SiphonConfig):
        self._config = config
        self._models: dict[str, type] = {}
        self._base = type("DynamicBase", (DeclarativeBase,), {})

    @property
    def base(self):
        """The DeclarativeBase subclass that all generated models inherit from."""
        return self._base

    @property
    def models(self) -> dict[str, type]:
        """Dict of table_name -> ORM model class."""
        return self._models

    def generate(self) -> dict[str, type]:
        """Generate all ORM models from config.

        1. Build data table models (from schema.tables + schema.fields)
           including FK columns for belongs_to relationships.
        2. Build junction table models.

        Returns dict of table_name -> ORM class.
        """
        self._build_data_tables()
        self._build_junction_tables()
        return self._models

    def _build_data_tables(self) -> None:
        """Build ORM classes for each data table defined in config.

        Groups fields by their target table, collects FK columns from
        belongs_to relationships, and creates the ORM class with all
        columns in a single pass.
        """
        # Group fields by table (include both top-level and collection fields)
        table_fields: dict[str, list] = {}
        for field in self._config.schema_.fields:
            table_name = field.db.table
            table_fields.setdefault(table_name, []).append(field)

        # Include collection fields so their target tables get proper columns
        if self._config.schema_.collections:
            for collection in self._config.schema_.collections:
                for field in collection.fields:
                    table_name = field.db.table
                    # Avoid duplicate columns (a field name may appear in both
                    # top-level and collection targeting the same table/column)
                    existing = table_fields.get(table_name, [])
                    if not any(f.db.column == field.db.column for f in existing):
                        table_fields.setdefault(table_name, []).append(field)

        # Collect FK columns per table from belongs_to relationships
        table_fks: dict[str, dict[str, Column]] = {}
        for rel in self._config.relationships:
            if isinstance(rel, BelongsToRelationship):
                ref_pk = self._config.schema_.tables[rel.references].primary_key
                if ref_pk.type == "auto_increment":
                    fk_type = Integer
                else:
                    fk_type = String(36)
                fk_col = Column(
                    fk_type,
                    ForeignKey(f"{rel.references}.{ref_pk.column}"),
                    nullable=True,
                )
                table_fks.setdefault(rel.table, {})[rel.fk_column] = fk_col

        # Build a model for each declared table
        for table_name, table_config in self._config.schema_.tables.items():
            pk = table_config.primary_key

            columns: dict[str, object] = {"__tablename__": table_name}

            # Primary key column
            if pk.type == "auto_increment":
                columns[pk.column] = Column(
                    Integer, primary_key=True, autoincrement=True
                )
            elif pk.type == "uuid":
                columns[pk.column] = Column(
                    String(36),
                    primary_key=True,
                    default=lambda: str(uuid.uuid4()),
                )

            # Data columns from fields
            for field in table_fields.get(table_name, []):
                sql_type = get_sql_type(field.type)
                columns[field.db.column] = Column(type(sql_type), nullable=True)

            # FK columns from belongs_to relationships
            for fk_col_name, fk_col in table_fks.get(table_name, {}).items():
                columns[fk_col_name] = fk_col

            # Add a UniqueConstraint for on_conflict.key so that SQLite/Postgres
            # can match the ON CONFLICT clause. Field names in on_conflict.key
            # are mapped to their underlying DB column names here.
            if table_config.on_conflict is not None:
                field_to_column = {
                    f.name: f.db.column for f in self._config.schema_.fields
                }
                if self._config.schema_.collections:
                    for coll in self._config.schema_.collections:
                        for f in coll.fields:
                            field_to_column.setdefault(f.name, f.db.column)
                conflict_columns = [
                    field_to_column.get(name, name)
                    for name in table_config.on_conflict.key
                ]
                # Skip the constraint when the conflict key is the primary key
                # (already unique by definition).
                if conflict_columns != [pk.column]:
                    columns["__table_args__"] = (
                        UniqueConstraint(
                            *conflict_columns,
                            name=f"uq_{table_name}_{'_'.join(conflict_columns)}",
                        ),
                    )

            # Create ORM class dynamically
            class_name = table_name.title().replace("_", "")
            model = type(class_name, (self._base,), columns)
            self._models[table_name] = model

    def _build_junction_tables(self) -> None:
        """Build ORM classes for junction (many-to-many) tables."""
        for rel in self._config.relationships:
            if isinstance(rel, JunctionRelationship):
                table1, table2 = rel.link
                pk1 = self._config.schema_.tables[table1].primary_key
                pk2 = self._config.schema_.tables[table2].primary_key

                col1_name = rel.columns[table1]
                col2_name = rel.columns[table2]

                col1_type = Integer if pk1.type == "auto_increment" else String(36)
                col2_type = Integer if pk2.type == "auto_increment" else String(36)

                columns: dict[str, object] = {
                    "__tablename__": rel.through,
                    col1_name: Column(
                        col1_type,
                        ForeignKey(f"{table1}.{pk1.column}"),
                        primary_key=True,
                    ),
                    col2_name: Column(
                        col2_type,
                        ForeignKey(f"{table2}.{pk2.column}"),
                        primary_key=True,
                    ),
                }

                class_name = rel.through.title().replace("_", "")
                model = type(class_name, (self._base,), columns)
                self._models[rel.through] = model
