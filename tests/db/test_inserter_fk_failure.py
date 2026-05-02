"""FK resolution failure behavior — what happens when a child references
a parent that doesn't exist?

These tests document the CURRENT behavior so future changes don't silently
break it. If the desired behavior changes (e.g., raise instead of NULL FK),
update these tests deliberately.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator


def _make_self_ref_config() -> SiphonConfig:
    """Companies table with self-referential parent_id FK."""
    return SiphonConfig.model_validate({
        "name": "fk-failure-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "company_name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "parent_entity", "source": "Parent", "type": "string",
                 "db": {"table": "companies", "column": "parent_name"}},
            ],
            "tables": {
                "companies": {"primary_key": {"column": "id", "type": "auto_increment"}},
            },
        },
        "relationships": [{
            "type": "belongs_to",
            "field": "parent_entity",
            "table": "companies",
            "references": "companies",
            "fk_column": "parent_id",
            "resolve_by": "name",
        }],
        "pipeline": {"review": False},
    })


@pytest.fixture
async def setup():
    config = _make_self_ref_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestFKResolutionFailure:
    async def test_unknown_parent_inserts_with_null_fk(self, setup):
        """A child whose parent_entity doesn't match anything in DB or cache:
        the row is inserted but parent_id is NULL.

        This is the current behavior. Documenting it explicitly so future
        changes are deliberate.
        """
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([
            {"company_name": "Orphan", "parent_entity": "NonExistentParent"},
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            rows = (await session.execute(
                select(companies.name, companies.parent_id)
            )).all()

        assert len(rows) == 1
        assert rows[0][0] == "Orphan"
        assert rows[0][1] is None  # FK is NULL because parent doesn't exist

    async def test_known_parent_resolves_correctly(self, setup):
        """Sanity check: when parent IS present, FK resolves to its PK."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([
            {"company_name": "Acme", "parent_entity": ""},
            {"company_name": "Acme West", "parent_entity": "Acme"},
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            rows = (await session.execute(
                select(companies.name, companies.id, companies.parent_id)
                .order_by(companies.id)
            )).all()

        assert rows[0][0] == "Acme"
        assert rows[0][2] is None
        assert rows[1][0] == "Acme West"
        assert rows[1][2] == rows[0][1]

    async def test_empty_parent_string_does_not_attempt_fk_lookup(self, setup):
        """An empty parent_entity string is treated as 'no parent' (NULL FK)."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([
            {"company_name": "Standalone", "parent_entity": ""},
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            rows = (await session.execute(
                select(companies.parent_id)
            )).all()

        assert rows[0][0] is None
