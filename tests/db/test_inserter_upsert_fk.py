"""Upsert + belongs_to FK resolution interaction.

When a parent table is configured with on_conflict=update and a row
is updated rather than inserted, child records (via belongs_to) must
still resolve their FK correctly. This test proves the lookup cache
works whether the parent was just inserted OR just updated.
"""

from __future__ import annotations

import pytest
from sqlalchemy import select

from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator


def _make_config() -> SiphonConfig:
    """Self-referential companies table with on_conflict=update on name."""
    return SiphonConfig.model_validate({
        "name": "upsert-fk-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "company_name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "parent_entity", "source": "Parent", "type": "string",
                 "db": {"table": "companies", "column": "parent_name"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "companies", "column": "phone"}},
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                    "on_conflict": {
                        "key": ["company_name"],
                        "action": "update",
                        "update_columns": "all",
                    },
                },
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
    config = _make_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestUpsertWithFK:
    async def test_child_fk_resolves_after_parent_upsert(self, setup):
        """First run: insert parent + child. Second run: update parent +
        insert another child. Both children should have the same parent_id."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        # First run: parent + child
        await inserter.insert([
            {"company_name": "Acme", "parent_entity": "", "phone": "OLD"},
            {"company_name": "Acme West", "parent_entity": "Acme", "phone": "111"},
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            rows = (await session.execute(
                select(companies.name, companies.id, companies.parent_id)
                .order_by(companies.id)
            )).all()
        acme_id = rows[0][1]
        first_child_parent_id = rows[1][2]
        assert first_child_parent_id == acme_id

        # Reset inserter to clear lookup cache (new pipeline run)
        inserter2 = Inserter(config, engine, model_gen)
        await inserter2.load_existing_keys()

        # Second run: update Acme + insert another child
        await inserter2.insert([
            {"company_name": "Acme", "parent_entity": "", "phone": "NEW"},
            {"company_name": "Acme East", "parent_entity": "Acme", "phone": "222"},
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            rows = (await session.execute(
                select(companies.name, companies.id, companies.parent_id, companies.phone)
                .order_by(companies.id)
            )).all()

        # Three rows: Acme (updated), Acme West, Acme East
        assert len(rows) == 3
        names = {r[0]: (r[1], r[2], r[3]) for r in rows}

        # Acme was updated, not duplicated; phone is now NEW
        assert names["Acme"][2] == "NEW"
        # Both children point to the same Acme id
        acme_id = names["Acme"][0]
        assert names["Acme West"][1] == acme_id
        assert names["Acme East"][1] == acme_id
