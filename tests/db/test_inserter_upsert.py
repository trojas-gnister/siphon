"""Integration tests for upsert behavior in the Inserter."""

from __future__ import annotations

import pytest
from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator
from siphon.utils.errors import DatabaseError


def _make_config(on_conflict: dict | None = None) -> SiphonConfig:
    """Build a minimal config with one table, optionally with on_conflict."""
    table_cfg = {
        "primary_key": {"column": "id", "type": "auto_increment"},
    }
    if on_conflict is not None:
        table_cfg["on_conflict"] = on_conflict

    return SiphonConfig.model_validate({
        "name": "upsert-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "companies", "column": "phone"}},
            ],
            "tables": {"companies": table_cfg},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def db_setup():
    """Create an in-memory SQLite DB with a unique constraint on name."""
    config = _make_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()

    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_companies_name"))

    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionError:
    async def test_default_action_raises_on_conflict(self, db_setup):
        """Without on_conflict, a duplicate insert raises DatabaseError."""
        config, engine, model_gen = db_setup
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "111"}])

        with pytest.raises(DatabaseError):
            await inserter.insert([{"name": "Acme", "phone": "222"}])


@pytest.fixture
async def db_setup_with_skip():
    """DB setup with on_conflict.action=skip configured."""
    config = _make_config({
        "key": ["name"],
        "action": "skip",
    })
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", name="uq_companies_name"))
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestActionSkip:
    async def test_skip_does_not_raise(self, db_setup_with_skip):
        """With action=skip, duplicate inserts are silently ignored."""
        config, engine, model_gen = db_setup_with_skip
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "111"}])
        await inserter.insert([{"name": "Acme", "phone": "222"}])

    async def test_skip_preserves_original_row(self, db_setup_with_skip):
        """The original row's values are unchanged after skip."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_with_skip
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "ORIGINAL"}])
        await inserter.insert([{"name": "Acme", "phone": "NEW"}])

        async with engine.session() as session:
            result = await session.execute(
                select(model_gen.models["companies"].phone)
            )
            phones = [r[0] for r in result]

        assert phones == ["ORIGINAL"]
