"""Tests for the Mapper — source data to target records."""

from __future__ import annotations

import re

import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.mapper import Mapper
from siphon.utils.errors import TransformError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _config_dict(fields, **overrides):
    """Return a minimal v2 config dict with the given fields."""
    base = {
        "name": "test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite:///test.db"},
        "schema": {
            "fields": fields,
            "tables": {"t": {"primary_key": {"column": "id", "type": "auto_increment"}}},
        },
    }
    base.update(overrides)
    return base


def _field(name, **kwargs):
    """Shorthand for a field dict with db defaulting to table 't'."""
    f = {"name": name, "db": {"table": "t", "column": name}}
    f.update(kwargs)
    return f


# ---------------------------------------------------------------------------
# 1. Direct source mapping
# ---------------------------------------------------------------------------

def test_direct_source_mapping():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("company", source="Company Name"),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"Company Name": "Acme Corp"})
    assert result == {"company": "Acme Corp"}


# ---------------------------------------------------------------------------
# 2. Alias matching
# ---------------------------------------------------------------------------

def test_alias_matching():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("company", source="Company Name", aliases=["Corp Name"]),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"Corp Name": "Acme Corp"})
    assert result == {"company": "Acme Corp"}


# ---------------------------------------------------------------------------
# 3. Case-insensitive source matching
# ---------------------------------------------------------------------------

def test_case_insensitive_source_matching():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("company", source="Company_Name"),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"company_name": "Acme Corp"})
    assert result == {"company": "Acme Corp"}


# ---------------------------------------------------------------------------
# 4. Case-insensitive alias matching
# ---------------------------------------------------------------------------

def test_case_insensitive_alias_matching():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("company", source="company_name", aliases=["CORP_NAME"]),
    ]))
    mapper = Mapper(cfg)
    # Source "company_name" doesn't match "Business" at all; alias "CORP_NAME"
    # matches "corp_name" case-insensitively.
    result = mapper.map_record({"corp_name": "Acme Corp"})
    assert result == {"company": "Acme Corp"}


# ---------------------------------------------------------------------------
# 5. Constant value
# ---------------------------------------------------------------------------

def test_constant_value():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("active", value=True),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"anything": "ignored"})
    assert result == {"active": True}


# ---------------------------------------------------------------------------
# 6. Template transform
# ---------------------------------------------------------------------------

def test_template_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("code", source="raw", transform={"type": "template", "template": "PREFIX-{value}"}),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"raw": "abc123"})
    assert result == {"code": "PREFIX-abc123"}


# ---------------------------------------------------------------------------
# 7. Template with variables
# ---------------------------------------------------------------------------

def test_template_with_variables():
    cfg = SiphonConfig.model_validate(_config_dict(
        [_field("code", source="raw", transform={"type": "template", "template": "{prefix}-{value}"})],
        variables={"prefix": "MFRM"},
    ))
    mapper = Mapper(cfg)
    result = mapper.map_record({"raw": "abc123"})
    assert result == {"code": "MFRM-abc123"}


# ---------------------------------------------------------------------------
# 8. Map transform
# ---------------------------------------------------------------------------

def test_map_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("status_id", source="status", transform={
            "type": "map",
            "values": {"Closed": 8, "Open": 3},
            "default": 0,
        }),
    ]))
    mapper = Mapper(cfg)
    assert mapper.map_record({"status": "Closed"}) == {"status_id": 8}
    assert mapper.map_record({"status": "Open"}) == {"status_id": 3}
    assert mapper.map_record({"status": "Unknown"}) == {"status_id": 0}


# ---------------------------------------------------------------------------
# 9. Concat transform
# ---------------------------------------------------------------------------

def test_concat_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("address", transform={
            "type": "concat",
            "fields": ["city", "state"],
            "separator": ", ",
        }),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"city": "Springfield", "state": "IL"})
    assert result == {"address": "Springfield, IL"}


# ---------------------------------------------------------------------------
# 10. UUID transform
# ---------------------------------------------------------------------------

def test_uuid_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("uid", transform={"type": "uuid"}),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({})
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    assert uuid_pattern.match(result["uid"])


# ---------------------------------------------------------------------------
# 11. Now transform
# ---------------------------------------------------------------------------

def test_now_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("ts", transform={"type": "now"}),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({})
    # Default format: "%Y-%m-%d %H:%M:%S"
    assert re.match(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", result["ts"])


# ---------------------------------------------------------------------------
# 12. Coalesce transform
# ---------------------------------------------------------------------------

def test_coalesce_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("phone", transform={
            "type": "coalesce",
            "fields": ["mobile", "home"],
        }),
    ]))
    mapper = Mapper(cfg)
    assert mapper.map_record({"mobile": None, "home": "555-1234"}) == {"phone": "555-1234"}
    assert mapper.map_record({"mobile": "555-0000", "home": "555-1234"}) == {"phone": "555-0000"}


# ---------------------------------------------------------------------------
# 13. Coalesce with fallback
# ---------------------------------------------------------------------------

def test_coalesce_with_fallback():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("phone", transform={
            "type": "coalesce",
            "fields": ["mobile", "home"],
            "fallback": {"type": "uuid"},
        }),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"mobile": None, "home": None})
    # Fallback is a uuid transform — result should be a valid UUID
    uuid_pattern = re.compile(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    )
    assert uuid_pattern.match(result["phone"])


# ---------------------------------------------------------------------------
# 14. Custom transform
# ---------------------------------------------------------------------------

def test_custom_transform():
    def double(x):
        return x * 2

    cfg = SiphonConfig.model_validate(_config_dict([
        _field("doubled", transform={
            "type": "custom",
            "function": "double",
            "args": ["amount"],
        }),
    ]))
    mapper = Mapper(cfg, custom_transforms={"double": double})
    result = mapper.map_record({"amount": 5})
    assert result == {"doubled": 10}


# ---------------------------------------------------------------------------
# 15. Missing custom transform raises TransformError
# ---------------------------------------------------------------------------

def test_missing_custom_transform_raises():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("x", transform={"type": "custom", "function": "nope", "args": []}),
    ]))
    mapper = Mapper(cfg)
    with pytest.raises(TransformError, match="Custom transform 'nope' not found"):
        mapper.map_record({})


# ---------------------------------------------------------------------------
# 16. Unknown transform type raises TransformError
# ---------------------------------------------------------------------------

def test_unknown_transform_raises():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("x", transform={"type": "bogus"}),
    ]))
    mapper = Mapper(cfg)
    with pytest.raises(TransformError, match="Unknown transform type: bogus"):
        mapper.map_record({})


# ---------------------------------------------------------------------------
# 17. Missing source column returns None
# ---------------------------------------------------------------------------

def test_missing_source_returns_none():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("company", source="Company Name"),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"other_field": "value"})
    assert result == {"company": None}


# ---------------------------------------------------------------------------
# 18. map_records — maps multiple records correctly
# ---------------------------------------------------------------------------

def test_map_records_multiple():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("name", source="Name"),
    ]))
    mapper = Mapper(cfg)
    records = [{"Name": "Alice"}, {"Name": "Bob"}, {"Name": "Carol"}]
    results = mapper.map_records(records)
    assert results == [{"name": "Alice"}, {"name": "Bob"}, {"name": "Carol"}]


# ---------------------------------------------------------------------------
# 19. No source, no value, no transform → None
# ---------------------------------------------------------------------------

def test_no_source_no_value_no_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("empty"),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"anything": "here"})
    assert result == {"empty": None}


# ---------------------------------------------------------------------------
# 20. Source + transform: source value read, then transform applied
# ---------------------------------------------------------------------------

def test_source_plus_transform():
    cfg = SiphonConfig.model_validate(_config_dict([
        _field("status_id", source="status", transform={
            "type": "map",
            "values": {"Active": 1, "Inactive": 0},
            "default": -1,
        }),
    ]))
    mapper = Mapper(cfg)
    result = mapper.map_record({"status": "Active"})
    assert result == {"status_id": 1}
