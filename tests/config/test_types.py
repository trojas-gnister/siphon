"""Tests for siphon/config/types.py — field type registry and preset resolution."""

from __future__ import annotations

import pytest
from sqlalchemy import types as sa_types

from siphon.config.types import (
    FIELD_TYPE_REGISTRY,
    get_formatter,
    get_sql_type,
    resolve_preset,
)
from siphon.utils.formatters import (
    format_boolean,
    format_country,
    format_currency,
    format_date,
    format_datetime,
    format_email,
    format_enum,
    format_integer,
    format_number,
    format_phone,
    format_regex,
    format_string,
    format_subdivision,
    format_url,
)


# ---------------------------------------------------------------------------
# FIELD_TYPE_REGISTRY structure
# ---------------------------------------------------------------------------


class TestFieldTypeRegistryStructure:
    """Verify the registry contains exactly the 14 expected type names."""

    EXPECTED_TYPES = {
        "string", "integer", "number", "currency",
        "phone", "url", "email", "date", "datetime",
        "enum", "boolean", "regex", "subdivision", "country",
    }

    def test_registry_has_all_14_types(self):
        assert set(FIELD_TYPE_REGISTRY.keys()) == self.EXPECTED_TYPES

    def test_each_entry_has_formatter_key(self):
        for type_name, entry in FIELD_TYPE_REGISTRY.items():
            assert "formatter" in entry, f"Missing 'formatter' key for type {type_name!r}"

    def test_each_entry_has_sql_type_key(self):
        for type_name, entry in FIELD_TYPE_REGISTRY.items():
            assert "sql_type" in entry, f"Missing 'sql_type' key for type {type_name!r}"

    def test_each_entry_has_options_key(self):
        for type_name, entry in FIELD_TYPE_REGISTRY.items():
            assert "options" in entry, f"Missing 'options' key for type {type_name!r}"

    def test_options_is_list(self):
        for type_name, entry in FIELD_TYPE_REGISTRY.items():
            assert isinstance(entry["options"], list), (
                f"'options' for type {type_name!r} should be a list"
            )

    def test_formatter_is_callable(self):
        for type_name, entry in FIELD_TYPE_REGISTRY.items():
            assert callable(entry["formatter"]), (
                f"'formatter' for type {type_name!r} should be callable"
            )


# ---------------------------------------------------------------------------
# get_formatter
# ---------------------------------------------------------------------------


class TestGetFormatter:
    def test_phone_returns_format_phone(self):
        assert get_formatter("phone") is format_phone

    def test_string_returns_format_string(self):
        assert get_formatter("string") is format_string

    def test_integer_returns_format_integer(self):
        assert get_formatter("integer") is format_integer

    def test_number_returns_format_number(self):
        assert get_formatter("number") is format_number

    def test_currency_returns_format_currency(self):
        assert get_formatter("currency") is format_currency

    def test_url_returns_format_url(self):
        assert get_formatter("url") is format_url

    def test_email_returns_format_email(self):
        assert get_formatter("email") is format_email

    def test_date_returns_format_date(self):
        assert get_formatter("date") is format_date

    def test_datetime_returns_format_datetime(self):
        assert get_formatter("datetime") is format_datetime

    def test_enum_returns_format_enum(self):
        assert get_formatter("enum") is format_enum

    def test_boolean_returns_format_boolean(self):
        assert get_formatter("boolean") is format_boolean

    def test_regex_returns_format_regex(self):
        assert get_formatter("regex") is format_regex

    def test_subdivision_returns_format_subdivision(self):
        assert get_formatter("subdivision") is format_subdivision

    def test_country_returns_format_country(self):
        assert get_formatter("country") is format_country

    def test_unknown_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown field type"):
            get_formatter("foobar")

    def test_empty_string_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown field type"):
            get_formatter("")

    def test_case_sensitive_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown field type"):
            get_formatter("String")


# ---------------------------------------------------------------------------
# get_sql_type
# ---------------------------------------------------------------------------


class TestGetSqlType:
    def test_currency_returns_numeric_12_2(self):
        sql_type = get_sql_type("currency")
        assert isinstance(sql_type, sa_types.Numeric)
        assert sql_type.precision == 12
        assert sql_type.scale == 2

    def test_string_returns_string_255(self):
        sql_type = get_sql_type("string")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 255

    def test_integer_returns_integer(self):
        sql_type = get_sql_type("integer")
        assert isinstance(sql_type, sa_types.Integer)

    def test_number_returns_float(self):
        sql_type = get_sql_type("number")
        assert isinstance(sql_type, sa_types.Float)

    def test_phone_returns_string_20(self):
        sql_type = get_sql_type("phone")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 20

    def test_url_returns_string_500(self):
        sql_type = get_sql_type("url")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 500

    def test_email_returns_string_255(self):
        sql_type = get_sql_type("email")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 255

    def test_date_returns_date(self):
        sql_type = get_sql_type("date")
        assert isinstance(sql_type, sa_types.Date)

    def test_datetime_returns_datetime(self):
        sql_type = get_sql_type("datetime")
        assert isinstance(sql_type, sa_types.DateTime)

    def test_enum_returns_string_50(self):
        sql_type = get_sql_type("enum")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 50

    def test_boolean_returns_boolean(self):
        sql_type = get_sql_type("boolean")
        assert isinstance(sql_type, sa_types.Boolean)

    def test_regex_returns_string_255(self):
        sql_type = get_sql_type("regex")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 255

    def test_subdivision_returns_string_10(self):
        sql_type = get_sql_type("subdivision")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 10

    def test_country_returns_string_2(self):
        sql_type = get_sql_type("country")
        assert isinstance(sql_type, sa_types.String)
        assert sql_type.length == 2

    def test_unknown_type_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown field type"):
            get_sql_type("notatype")

    def test_empty_string_raises_value_error(self):
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
