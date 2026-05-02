"""Tests for multi-source / join schema models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from siphon.config.schema import JoinConfig, SiphonConfig, SourceConfig


def _table(name: str = "t") -> dict:
    return {"primary_key": {"column": "id", "type": "auto_increment"}}


class TestJoinConfig:
    def test_left_join(self):
        j = JoinConfig(left="a", right="b", on="key", type="left")
        assert j.type == "left"
        assert j.left == "a"
        assert j.right == "b"
        assert j.on == "key"

    def test_inner_join(self):
        j = JoinConfig(left="a", right="b", on="key", type="inner")
        assert j.type == "inner"

    def test_invalid_type_rejected(self):
        with pytest.raises(ValidationError):
            JoinConfig(left="a", right="b", on="key", type="cross")

    def test_default_type_is_left(self):
        j = JoinConfig(left="a", right="b", on="key")
        assert j.type == "left"


class TestSourceConfigMultiSource:
    def test_source_config_accepts_name_path_fields(self):
        sc = SourceConfig(
            type="spreadsheet",
            name="companies",
            path="./companies.csv",
            fields=[{
                "name": "company_name",
                "source": "Name",
                "type": "string",
                "db": {"table": "companies", "column": "name"},
            }],
        )
        assert sc.name == "companies"
        assert sc.path == "./companies.csv"
        assert sc.fields is not None
        assert len(sc.fields) == 1


class TestSiphonConfigMultiSource:
    def test_single_source_config_unchanged(self):
        """Existing source: + schema.fields: shape still works."""
        cfg = SiphonConfig.model_validate({
            "name": "single",
            "source": {"type": "spreadsheet"},
            "database": {"url": "sqlite:///t.db"},
            "schema": {
                "fields": [{
                    "name": "n", "source": "N", "type": "string",
                    "db": {"table": "t", "column": "n"},
                }],
                "tables": {"t": _table()},
            },
        })
        assert cfg.source is not None
        assert cfg.sources is None or cfg.sources == []

    def test_multi_source_config(self):
        """sources: list with per-source fields and a join."""
        cfg = SiphonConfig.model_validate({
            "name": "multi",
            "sources": [
                {
                    "name": "companies",
                    "type": "spreadsheet",
                    "path": "./companies.csv",
                    "fields": [{
                        "name": "company_name", "source": "Name", "type": "string",
                        "db": {"table": "companies", "column": "name"},
                    }, {
                        "name": "company_code", "source": "Code", "type": "string",
                    }],
                },
                {
                    "name": "addresses",
                    "type": "spreadsheet",
                    "path": "./addresses.csv",
                    "fields": [{
                        "name": "address", "source": "Address", "type": "string",
                        "db": {"table": "addresses", "column": "full_address"},
                    }, {
                        "name": "company_code", "source": "Company Code", "type": "string",
                    }],
                },
            ],
            "joins": [
                {"left": "companies", "right": "addresses", "on": "company_code"},
            ],
            "database": {"url": "sqlite:///t.db"},
            "schema": {
                "tables": {
                    "companies": _table(),
                    "addresses": _table(),
                },
            },
        })
        assert cfg.sources is not None
        assert len(cfg.sources) == 2
        assert cfg.joins is not None
        assert len(cfg.joins) == 1
        assert cfg.joins[0].on == "company_code"

    def test_both_source_and_sources_rejected(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "source": {"type": "spreadsheet"},
                "sources": [{
                    "name": "x", "type": "spreadsheet", "path": "./x.csv",
                    "fields": [{
                        "name": "a", "source": "A", "type": "string",
                        "db": {"table": "t", "column": "a"},
                    }],
                }],
                "database": {"url": "sqlite:///t.db"},
                "schema": {
                    "fields": [],
                    "tables": {"t": _table()},
                },
            })

    def test_neither_source_nor_sources_rejected(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "database": {"url": "sqlite:///t.db"},
                "schema": {
                    "fields": [],
                    "tables": {"t": _table()},
                },
            })

    def test_sources_must_have_name(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "sources": [{
                    "type": "spreadsheet", "path": "./x.csv",
                    "fields": [],
                }],
                "database": {"url": "sqlite:///t.db"},
                "schema": {"tables": {"t": _table()}},
            })

    def test_sources_must_have_fields(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "sources": [{
                    "name": "x", "type": "spreadsheet", "path": "./x.csv",
                }],
                "database": {"url": "sqlite:///t.db"},
                "schema": {"tables": {"t": _table()}},
            })

    def test_sources_with_unique_names(self):
        with pytest.raises(ValidationError):
            SiphonConfig.model_validate({
                "name": "bad",
                "sources": [
                    {"name": "x", "type": "spreadsheet", "path": "./a.csv", "fields": [{
                        "name": "a", "source": "A", "type": "string",
                        "db": {"table": "t", "column": "a"},
                    }]},
                    {"name": "x", "type": "spreadsheet", "path": "./b.csv", "fields": [{
                        "name": "b", "source": "B", "type": "string",
                        "db": {"table": "t", "column": "b"},
                    }]},
                ],
                "database": {"url": "sqlite:///t.db"},
                "schema": {"tables": {"t": _table()}},
            })
