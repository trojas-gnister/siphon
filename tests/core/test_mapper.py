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


# ---------------------------------------------------------------------------
# Helpers for collection tests
# ---------------------------------------------------------------------------

def _collection_config_dict(fields, collections, **overrides):
    """Return a config dict that includes both parent fields and collections."""
    tables = {
        "ws_cases": {"primary_key": {"column": "id", "type": "auto_increment"}},
        "ws_incident_notes": {"primary_key": {"column": "id", "type": "auto_increment"}},
    }
    base = {
        "name": "test",
        "source": {"type": "xml"},
        "database": {"url": "sqlite:///test.db"},
        "schema": {
            "fields": fields,
            "collections": collections,
            "tables": tables,
        },
    }
    base.update(overrides)
    return base


def _parent_field(name, **kwargs):
    f = {"name": name, "db": {"table": "ws_cases", "column": name}}
    f.update(kwargs)
    return f


def _note_field(name, **kwargs):
    f = {"name": name, "db": {"table": "ws_incident_notes", "column": name}}
    f.update(kwargs)
    return f


# ---------------------------------------------------------------------------
# 21. Collection expansion tests
# ---------------------------------------------------------------------------


class TestCollectionExpansion:
    """Tests for Mapper.map_collections() and Mapper._navigate_path()."""

    # -- source record fixture used by multiple tests -----------------------
    SOURCE_RECORD = {
        "CaseCode": "abc-123",
        "CaseNotes": {
            "CaseNote": [
                {"CaseNote": "First note", "Date": "2025-01-01", "UserName": "Smith, John"},
                {"CaseNote": "Second note", "Date": "2025-01-02", "UserName": "Doe, Jane"},
            ]
        },
    }

    def _make_mapper(self, collections=None):
        """Build a Mapper with a minimal parent field and the given collections."""
        cfg = SiphonConfig.model_validate(_collection_config_dict(
            fields=[_parent_field("case_code", source="CaseCode")],
            collections=collections or [
                {
                    "name": "case_notes",
                    "source_path": "CaseNotes.CaseNote",
                    "fields": [
                        _note_field("note", source="CaseNote"),
                        _note_field("created_date", source="Date"),
                    ],
                }
            ],
        ))
        return Mapper(cfg)

    def test_basic_collection_expansion(self):
        """Nested list produces multiple mapped records."""
        mapper = self._make_mapper()
        parent_mapped = {"case_code": "abc-123"}
        result = mapper.map_collections(self.SOURCE_RECORD, parent_mapped)

        assert "case_notes" in result
        notes = result["case_notes"]
        assert len(notes) == 2
        assert notes[0] == {"note": "First note", "created_date": "2025-01-01"}
        assert notes[1] == {"note": "Second note", "created_date": "2025-01-02"}

    def test_single_item_not_list(self):
        """Single nested dict (not list) is treated as a 1-item list."""
        source = {
            "CaseCode": "xyz-999",
            "CaseNotes": {
                "CaseNote": {"CaseNote": "Only note", "Date": "2025-03-01"},
            },
        }
        mapper = self._make_mapper()
        result = mapper.map_collections(source, {"case_code": "xyz-999"})

        notes = result["case_notes"]
        assert len(notes) == 1
        assert notes[0]["note"] == "Only note"

    def test_collection_field_from_item(self):
        """Collection fields are read from the nested item, not the parent."""
        mapper = self._make_mapper()
        result = mapper.map_collections(self.SOURCE_RECORD, {})

        # "CaseNote" key lives inside each nested item, not on the parent
        assert result["case_notes"][0]["note"] == "First note"
        assert result["case_notes"][1]["note"] == "Second note"

    def test_collection_field_from_parent(self):
        """Collection field that doesn't exist on the item falls back to parent record."""
        cfg = SiphonConfig.model_validate(_collection_config_dict(
            fields=[_parent_field("case_code", source="CaseCode")],
            collections=[
                {
                    "name": "case_notes",
                    "source_path": "CaseNotes.CaseNote",
                    "fields": [
                        _note_field("note", source="CaseNote"),
                        # CaseCode lives on the parent, not inside each CaseNote item
                        _note_field("case_ref", source="CaseCode"),
                    ],
                }
            ],
        ))
        mapper = Mapper(cfg)
        result = mapper.map_collections(self.SOURCE_RECORD, {"case_code": "abc-123"})

        for note in result["case_notes"]:
            assert note["case_ref"] == "abc-123"

    def test_collection_with_transform(self):
        """Transform is applied to collection field values."""
        def reverse_name(name):
            # "Smith, John" -> "John Smith"
            if name and "," in name:
                last, first = name.split(",", 1)
                return f"{first.strip()} {last.strip()}"
            return name

        cfg = SiphonConfig.model_validate(_collection_config_dict(
            fields=[_parent_field("case_code", source="CaseCode")],
            collections=[
                {
                    "name": "case_notes",
                    "source_path": "CaseNotes.CaseNote",
                    "fields": [
                        _note_field("note", source="CaseNote"),
                        _note_field("created_by", source="UserName", transform={
                            "type": "custom",
                            "function": "reverse_name",
                            "args": ["UserName"],
                        }),
                    ],
                }
            ],
        ))
        mapper = Mapper(cfg, custom_transforms={"reverse_name": reverse_name})
        result = mapper.map_collections(self.SOURCE_RECORD, {})

        assert result["case_notes"][0]["created_by"] == "John Smith"
        assert result["case_notes"][1]["created_by"] == "Jane Doe"

    def test_collection_with_constant_value(self):
        """Constant value fields in collections resolve correctly."""
        cfg = SiphonConfig.model_validate(_collection_config_dict(
            fields=[_parent_field("case_code", source="CaseCode")],
            collections=[
                {
                    "name": "case_notes",
                    "source_path": "CaseNotes.CaseNote",
                    "fields": [
                        _note_field("note", source="CaseNote"),
                        _note_field("active", value=True),
                    ],
                }
            ],
        ))
        mapper = Mapper(cfg)
        result = mapper.map_collections(self.SOURCE_RECORD, {})

        for note in result["case_notes"]:
            assert note["active"] is True

    def test_empty_collection(self):
        """Missing nested path produces no entry for that collection."""
        source = {"CaseCode": "no-notes"}  # No CaseNotes key at all
        mapper = self._make_mapper()
        result = mapper.map_collections(source, {"case_code": "no-notes"})

        # Path not found → collection omitted from result
        assert "case_notes" not in result

    def test_no_collections_configured(self):
        """Returns empty dict when no collections defined in config."""
        cfg = SiphonConfig.model_validate(_config_dict([
            _field("name", source="Name"),
        ]))
        mapper = Mapper(cfg)
        result = mapper.map_collections({"Name": "Alice"}, {"name": "Alice"})
        assert result == {}

    def test_navigate_path_basic(self):
        """Dot-separated path navigates nested dicts correctly."""
        cfg = SiphonConfig.model_validate(_config_dict([_field("x")]))
        mapper = Mapper(cfg)

        data = {"A": {"B": {"C": [1, 2, 3]}}}
        assert mapper._navigate_path(data, "A.B.C") == [1, 2, 3]
        assert mapper._navigate_path(data, "A.B") == {"C": [1, 2, 3]}
        assert mapper._navigate_path(data, "A") == {"B": {"C": [1, 2, 3]}}

    def test_navigate_path_missing(self):
        """Missing path segment returns None."""
        cfg = SiphonConfig.model_validate(_config_dict([_field("x")]))
        mapper = Mapper(cfg)

        data = {"A": {"B": 42}}
        assert mapper._navigate_path(data, "A.X") is None
        assert mapper._navigate_path(data, "Z") is None
        assert mapper._navigate_path(data, "A.B.C") is None  # 42 is not a dict

    def test_parent_context_available_in_transforms(self):
        """Transform args can reference parent record fields via merged context."""
        def combine(note_text, case_code):
            return f"[{case_code}] {note_text}"

        cfg = SiphonConfig.model_validate(_collection_config_dict(
            fields=[_parent_field("case_code", source="CaseCode")],
            collections=[
                {
                    "name": "case_notes",
                    "source_path": "CaseNotes.CaseNote",
                    "fields": [
                        _note_field("note", source="CaseNote", transform={
                            "type": "custom",
                            "function": "combine",
                            "args": ["CaseNote", "CaseCode"],
                        }),
                    ],
                }
            ],
        ))
        mapper = Mapper(cfg, custom_transforms={"combine": combine})
        result = mapper.map_collections(self.SOURCE_RECORD, {})

        # CaseCode comes from parent context; CaseNote from the item
        assert result["case_notes"][0]["note"] == "[abc-123] First note"
        assert result["case_notes"][1]["note"] == "[abc-123] Second note"
