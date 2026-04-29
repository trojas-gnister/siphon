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


@pytest.fixture
async def db_setup_with_update_all():
    """DB setup with on_conflict.action=update, update_columns=all."""
    config = _make_config({
        "key": ["name"],
        "action": "update",
        "update_columns": "all",
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


class TestActionUpdateAll:
    async def test_update_changes_all_non_key_columns(self, db_setup_with_update_all):
        """On conflict, all non-key columns are updated."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_with_update_all
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "ORIGINAL"}])
        await inserter.insert([{"name": "Acme", "phone": "UPDATED"}])

        async with engine.session() as session:
            result = await session.execute(
                select(model_gen.models["companies"].phone)
            )
            phones = [r[0] for r in result]

        assert phones == ["UPDATED"]

    async def test_update_does_not_create_duplicate_row(self, db_setup_with_update_all):
        """Upsert preserves the original row's PK; no duplicate is inserted."""
        from sqlalchemy import select, func
        config, engine, model_gen = db_setup_with_update_all
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "phone": "111"}])
        await inserter.insert([{"name": "Acme", "phone": "222"}])

        async with engine.session() as session:
            result = await session.execute(
                select(func.count()).select_from(model_gen.models["companies"])
            )
            count = result.scalar()

        assert count == 1


def _make_config_with_extra_column(on_conflict: dict | None = None) -> SiphonConfig:
    """Config with an extra 'website' column for selective-update testing."""
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
                {"name": "website", "source": "Website", "type": "string",
                 "db": {"table": "companies", "column": "website"}},
            ],
            "tables": {"companies": table_cfg},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def db_setup_with_update_specific():
    """DB setup with update_columns=['phone'] only."""
    config = _make_config_with_extra_column({
        "key": ["name"],
        "action": "update",
        "update_columns": ["phone"],
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


class TestActionUpdateSpecific:
    async def test_only_listed_columns_are_updated(self, db_setup_with_update_specific):
        """update_columns=['phone'] updates phone but leaves website unchanged."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_with_update_specific
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([
            {"name": "Acme", "phone": "111", "website": "ORIGINAL"}
        ])
        await inserter.insert([
            {"name": "Acme", "phone": "222", "website": "NEW"}
        ])

        async with engine.session() as session:
            companies = model_gen.models["companies"]
            result = await session.execute(
                select(companies.phone, companies.website)
            )
            row = result.one()

        assert row.phone == "222"  # updated
        assert row.website == "ORIGINAL"  # not in update_columns, unchanged


def _make_config_with_composite_key(on_conflict: dict) -> SiphonConfig:
    """Config with two-column unique key."""
    return SiphonConfig.model_validate({
        "name": "upsert-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "name"}},
                {"name": "country_code", "source": "Country", "type": "string",
                 "required": True, "db": {"table": "companies", "column": "country_code"}},
                {"name": "phone", "source": "Phone", "type": "string",
                 "db": {"table": "companies", "column": "phone"}},
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                    "on_conflict": on_conflict,
                },
            },
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def db_setup_composite_key():
    config = _make_config_with_composite_key({
        "key": ["name", "country_code"],
        "action": "update",
    })
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    from sqlalchemy import UniqueConstraint
    table = model_gen.models["companies"].__table__
    table.append_constraint(UniqueConstraint("name", "country_code", name="uq_name_country"))
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestCompositeKey:
    async def test_same_name_different_country_inserts_separately(self, db_setup_composite_key):
        """Same name, different country = two separate rows."""
        from sqlalchemy import select, func
        config, engine, model_gen = db_setup_composite_key
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "country_code": "US", "phone": "1"}])
        await inserter.insert([{"name": "Acme", "country_code": "CA", "phone": "2"}])

        async with engine.session() as session:
            result = await session.execute(
                select(func.count()).select_from(model_gen.models["companies"])
            )
            assert result.scalar() == 2

    async def test_same_name_and_country_updates(self, db_setup_composite_key):
        """Same name AND country = update existing row."""
        from sqlalchemy import select
        config, engine, model_gen = db_setup_composite_key
        inserter = Inserter(config, engine, model_gen)

        await inserter.insert([{"name": "Acme", "country_code": "US", "phone": "OLD"}])
        await inserter.insert([{"name": "Acme", "country_code": "US", "phone": "NEW"}])

        async with engine.session() as session:
            result = await session.execute(
                select(model_gen.models["companies"].phone)
            )
            phones = [r[0] for r in result]

        assert phones == ["NEW"]
