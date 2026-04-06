"""Tests for built-in transform functions."""

from __future__ import annotations

import re
import time

import pytest

from siphon.transforms.builtins import (
    BUILTIN_TRANSFORMS,
    transform_coalesce,
    transform_concat,
    transform_map,
    transform_now,
    transform_template,
    transform_uuid,
)


# ---------------------------------------------------------------------------
# transform_template
# ---------------------------------------------------------------------------

class TestTransformTemplate:
    def test_basic_value_placeholder(self):
        result = transform_template("abc-123", template="{value}", context={})
        assert result == "abc-123"

    def test_context_variable_with_value(self):
        result = transform_template(
            "abc-123", template="{prefix}-{value}", context={"prefix": "MFRM"}
        )
        assert result == "MFRM-abc-123"

    def test_none_value_treated_as_empty_string(self):
        result = transform_template(None, template="ID:{value}", context={})
        assert result == "ID:"

    def test_context_only_no_value_placeholder(self):
        result = transform_template(
            "ignored", template="{org}-{dept}", context={"org": "ACME", "dept": "HR"}
        )
        assert result == "ACME-HR"

    def test_multiple_context_keys(self):
        result = transform_template(
            42,
            template="{a}|{b}|{value}",
            context={"a": "X", "b": "Y"},
        )
        assert result == "X|Y|42"

    def test_returns_string(self):
        result = transform_template(100, template="{value}", context={})
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# transform_map
# ---------------------------------------------------------------------------

class TestTransformMap:
    def test_value_found_returns_mapped(self):
        result = transform_map("Closed", values={"Closed": 8, "Open": 3}, default=0)
        assert result == 8

    def test_value_not_found_returns_default(self):
        result = transform_map("Unknown", values={"Closed": 8}, default=0)
        assert result == 0

    def test_none_value_returns_default(self):
        result = transform_map(None, values={"Closed": 8}, default=0)
        assert result == 0

    def test_no_default_specified_returns_none(self):
        result = transform_map("Missing", values={"Closed": 8})
        assert result is None

    def test_integer_value_coerced_to_string_key(self):
        result = transform_map(1, values={"1": "one"}, default="nope")
        assert result == "one"

    def test_integer_mapped_values(self):
        result = transform_map("Open", values={"Open": 3, "Closed": 8}, default=-1)
        assert result == 3

    def test_none_default_explicit(self):
        result = transform_map("x", values={}, default=None)
        assert result is None


# ---------------------------------------------------------------------------
# transform_concat
# ---------------------------------------------------------------------------

class TestTransformConcat:
    def test_multiple_values_default_separator(self):
        result = transform_concat(fields=["hello", "world"])
        assert result == "hello world"

    def test_custom_separator(self):
        result = transform_concat(
            fields=["123 Main", "Springfield", "IL"], separator=", "
        )
        assert result == "123 Main, Springfield, IL"

    def test_none_values_skipped(self):
        result = transform_concat(fields=["hello", None, "world"])
        assert result == "hello world"

    def test_empty_string_values_skipped(self):
        result = transform_concat(fields=["hello", "", "world"])
        assert result == "hello world"

    def test_whitespace_only_values_skipped(self):
        result = transform_concat(fields=["hello", "   ", "world"])
        assert result == "hello world"

    def test_all_empty_returns_empty_string(self):
        result = transform_concat(fields=[None, "", "   "])
        assert result == ""

    def test_single_value_no_separator(self):
        result = transform_concat(fields=["only"])
        assert result == "only"

    def test_empty_fields_list_returns_empty_string(self):
        result = transform_concat(fields=[])
        assert result == ""

    def test_mixed_types_converted_to_string(self):
        result = transform_concat(fields=[1, 2, 3], separator="-")
        assert result == "1-2-3"


# ---------------------------------------------------------------------------
# transform_uuid
# ---------------------------------------------------------------------------

UUID4_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)


class TestTransformUuid:
    def test_returns_string(self):
        assert isinstance(transform_uuid(), str)

    def test_matches_uuid4_format(self):
        result = transform_uuid()
        assert UUID4_PATTERN.match(result), f"UUID did not match expected format: {result}"

    def test_two_calls_return_different_values(self):
        assert transform_uuid() != transform_uuid()

    def test_lowercase(self):
        result = transform_uuid()
        assert result == result.lower()


# ---------------------------------------------------------------------------
# transform_now
# ---------------------------------------------------------------------------

class TestTransformNow:
    def test_returns_string(self):
        assert isinstance(transform_now(), str)

    def test_default_format(self):
        result = transform_now()
        # Should parse cleanly as "%Y-%m-%d %H:%M:%S"
        from datetime import datetime
        parsed = datetime.strptime(result, "%Y-%m-%d %H:%M:%S")
        assert parsed is not None

    def test_custom_format_date_only(self):
        result = transform_now(fmt="%Y-%m-%d")
        assert re.match(r"^\d{4}-\d{2}-\d{2}$", result)

    def test_custom_format_iso(self):
        result = transform_now(fmt="%Y-%m-%dT%H:%M:%S")
        assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$", result)

    def test_value_changes_over_time(self):
        """Two calls separated by >1s should differ (or at least both are valid)."""
        r1 = transform_now()
        time.sleep(1.1)
        r2 = transform_now()
        assert r1 != r2


# ---------------------------------------------------------------------------
# transform_coalesce
# ---------------------------------------------------------------------------

class TestTransformCoalesce:
    def test_first_non_null_returned(self):
        result = transform_coalesce(fields=["hello", "world"])
        assert result == "hello"

    def test_skips_none(self):
        result = transform_coalesce(fields=[None, "hello"])
        assert result == "hello"

    def test_skips_empty_string(self):
        result = transform_coalesce(fields=["", "hello"])
        assert result == "hello"

    def test_skips_whitespace_only(self):
        result = transform_coalesce(fields=["   ", "hello"])
        assert result == "hello"

    def test_all_null_returns_fallback(self):
        result = transform_coalesce(fields=[None, None], fallback="default")
        assert result == "default"

    def test_no_fallback_returns_none(self):
        result = transform_coalesce(fields=[None, ""])
        assert result is None

    def test_first_value_valid_returns_immediately(self):
        result = transform_coalesce(fields=["first", "second", "third"])
        assert result == "first"

    def test_integer_value_returned(self):
        result = transform_coalesce(fields=[None, 0, 42])
        # 0 is falsy but str(0).strip() == "0" which is truthy
        assert result == 0

    def test_empty_fields_returns_fallback(self):
        result = transform_coalesce(fields=[], fallback="fallback")
        assert result == "fallback"


# ---------------------------------------------------------------------------
# BUILTIN_TRANSFORMS registry
# ---------------------------------------------------------------------------

class TestBuiltinTransformsRegistry:
    def test_has_all_six_keys(self):
        expected = {"template", "map", "concat", "uuid", "now", "coalesce"}
        assert set(BUILTIN_TRANSFORMS.keys()) == expected

    def test_all_values_are_callable(self):
        for name, fn in BUILTIN_TRANSFORMS.items():
            assert callable(fn), f"BUILTIN_TRANSFORMS['{name}'] is not callable"

    def test_template_key_returns_correct_function(self):
        assert BUILTIN_TRANSFORMS["template"] is transform_template

    def test_map_key_returns_correct_function(self):
        assert BUILTIN_TRANSFORMS["map"] is transform_map

    def test_concat_key_returns_correct_function(self):
        assert BUILTIN_TRANSFORMS["concat"] is transform_concat

    def test_uuid_key_returns_correct_function(self):
        assert BUILTIN_TRANSFORMS["uuid"] is transform_uuid

    def test_now_key_returns_correct_function(self):
        assert BUILTIN_TRANSFORMS["now"] is transform_now

    def test_coalesce_key_returns_correct_function(self):
        assert BUILTIN_TRANSFORMS["coalesce"] is transform_coalesce
