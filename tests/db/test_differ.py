"""Tests for the Differ — dry-run diff against existing DB state."""

from __future__ import annotations

import pytest
from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.differ import Differ
from siphon.db.models import ModelGenerator


def _make_config(on_conflict: dict | None = None) -> SiphonConfig:
    """Build a minimal config with one table, optionally with on_conflict."""
    table_cfg = {"primary_key": {"column": "id", "type": "auto_increment"}}
    if on_conflict is not None:
        table_cfg["on_conflict"] = on_conflict
    return SiphonConfig.model_validate({
        "name": "diff-test",
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
async def diff_setup():
    config = _make_config({"key": ["name"], "action": "update"})
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestDifferConstruction:
    async def test_differ_can_be_constructed(self, diff_setup):
        config, engine, model_gen = diff_setup
        differ = Differ(config, engine, model_gen)
        assert differ is not None

    async def test_compute_diff_returns_dict_with_categories(self, diff_setup):
        config, engine, model_gen = diff_setup
        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([])
        assert "insert" in result
        assert "update" in result
        assert "skip" in result
        assert "no_change" in result
        assert result["insert"] == []
        assert result["update"] == []
        assert result["skip"] == []
        assert result["no_change"] == []


@pytest.fixture
async def diff_setup_no_conflict():
    """Setup without on_conflict — every record is an insert."""
    config = _make_config()  # no on_conflict
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestNoOnConflict:
    async def test_records_categorized_as_insert(self, diff_setup_no_conflict):
        config, engine, model_gen = diff_setup_no_conflict
        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([
            {"name": "Acme", "phone": "111"},
            {"name": "Beta", "phone": "222"},
        ])
        assert len(result["insert"]) == 2
        assert result["update"] == []
        assert result["skip"] == []
        assert result["no_change"] == []


class TestUpdateVsNoChange:
    async def test_existing_row_with_changed_values_is_update(self, diff_setup):
        """An existing row whose values differ should be categorized as 'update'."""
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup

        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([{"name": "Acme", "phone": "OLD"}])

        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([{"name": "Acme", "phone": "NEW"}])

        assert len(result["update"]) == 1
        assert result["update"][0]["key"] == {"name": "Acme"}
        assert result["update"][0]["changes"] == {"phone": {"old": "OLD", "new": "NEW"}}
        assert result["update"][0]["record"] == {"name": "Acme", "phone": "NEW"}
        assert result["insert"] == []
        assert result["no_change"] == []

    async def test_existing_row_with_same_values_is_no_change(self, diff_setup):
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup

        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([{"name": "Acme", "phone": "111"}])

        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([{"name": "Acme", "phone": "111"}])

        assert len(result["no_change"]) == 1
        assert result["update"] == []
        assert result["insert"] == []

    async def test_new_row_is_insert(self, diff_setup):
        """A row whose key isn't in the DB should be 'insert' even with on_conflict set."""
        differ = Differ(*diff_setup)
        result = await differ.compute_diff([{"name": "BrandNew", "phone": "999"}])
        assert len(result["insert"]) == 1
        assert result["update"] == []
        assert result["no_change"] == []

    async def test_multiple_records_categorized_correctly(self, diff_setup):
        from siphon.db.inserter import Inserter
        config, engine, model_gen = diff_setup

        inserter = Inserter(config, engine, model_gen)
        await inserter.insert([
            {"name": "Existing-Same", "phone": "111"},
            {"name": "Existing-Changed", "phone": "OLD"},
        ])

        differ = Differ(config, engine, model_gen)
        result = await differ.compute_diff([
            {"name": "Existing-Same", "phone": "111"},
            {"name": "Existing-Changed", "phone": "NEW"},
            {"name": "Brand-New", "phone": "222"},
        ])

        assert len(result["insert"]) == 1
        assert result["insert"][0]["name"] == "Brand-New"
        assert len(result["update"]) == 1
        assert result["update"][0]["record"]["name"] == "Existing-Changed"
        assert len(result["no_change"]) == 1
        assert result["no_change"][0]["name"] == "Existing-Same"
