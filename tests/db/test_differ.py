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
