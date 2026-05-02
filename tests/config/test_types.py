"""Tests for siphon/config/types.py — field type registry and preset resolution."""

from __future__ import annotations

import pytest

from siphon.config.types import (
    get_formatter,
    get_sql_type,
    resolve_preset,
)


# ---------------------------------------------------------------------------
# get_formatter — error paths
# ---------------------------------------------------------------------------


def test_get_formatter_unknown_type_raises_value_error():
    with pytest.raises(ValueError, match="Unknown field type"):
        get_formatter("foobar")


def test_get_formatter_empty_string_raises_value_error():
    with pytest.raises(ValueError, match="Unknown field type"):
        get_formatter("")


def test_get_formatter_case_sensitive_raises_value_error():
    with pytest.raises(ValueError, match="Unknown field type"):
        get_formatter("String")


# ---------------------------------------------------------------------------
# get_sql_type — error paths
# ---------------------------------------------------------------------------


def test_get_sql_type_unknown_type_raises_value_error():
    with pytest.raises(ValueError, match="Unknown field type"):
        get_sql_type("notatype")


def test_get_sql_type_empty_string_raises_value_error():
    with pytest.raises(ValueError, match="Unknown field type"):
        get_sql_type("")


# ---------------------------------------------------------------------------
# resolve_preset
# ---------------------------------------------------------------------------


class TestResolvePreset:
    def test_us_states_returns_list_of_strings(self):
        result = resolve_preset("us_states")
        assert isinstance(result, list)
        assert all(isinstance(code, str) for code in result)

    def test_us_states_contains_expected_codes(self):
        result = resolve_preset("us_states")
        # Well-known US state codes that must be present
        for code in ("CA", "TX", "NY", "FL", "WA"):
            assert code in result, f"Expected US state code {code!r} in result"

    def test_us_states_returns_sorted_list(self):
        result = resolve_preset("us_states")
        assert result == sorted(result)

    def test_us_states_result_is_nonempty(self):
        result = resolve_preset("us_states")
        # US has 50 states + DC + territories; should be well above 50
        assert len(result) >= 50

    def test_us_states_codes_are_uppercase(self):
        result = resolve_preset("us_states")
        for code in result:
            assert code == code.upper(), f"Expected uppercase code, got {code!r}"

    def test_us_states_codes_have_no_hyphen(self):
        """Codes should be the local part only (e.g. 'CA', not 'US-CA')."""
        result = resolve_preset("us_states")
        for code in result:
            assert "-" not in code, f"Code should not contain hyphen: {code!r}"

    def test_ca_provinces_returns_list(self):
        result = resolve_preset("ca_provinces")
        assert isinstance(result, list)
        assert len(result) > 0

    def test_ca_provinces_contains_expected_codes(self):
        result = resolve_preset("ca_provinces")
        for code in ("ON", "BC", "AB", "QC"):
            assert code in result, f"Expected CA province code {code!r} in result"

    def test_ca_provinces_returns_sorted_list(self):
        result = resolve_preset("ca_provinces")
        assert result == sorted(result)

    def test_unknown_preset_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown preset"):
            resolve_preset("zz_regions")

    def test_empty_preset_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown preset"):
            resolve_preset("")

    def test_unknown_preset_error_message_is_informative(self):
        with pytest.raises(ValueError, match="zz_regions"):
            resolve_preset("zz_regions")
