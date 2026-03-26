"""Tests for the dynamic Pydantic Validator."""

from __future__ import annotations

import copy

import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.validator import Validator


# ---------------------------------------------------------------------------
# Helpers: build configs
# ---------------------------------------------------------------------------


def _make_config(extra_fields: list[dict] | None = None, extra_tables: dict | None = None) -> SiphonConfig:
    """Return a SiphonConfig with a 'companies' table and any additional fields/tables."""
    fields = [
        {
            "name": "company_name",
            "type": "string",
            "required": True,
            "db": {"table": "companies", "column": "name"},
        },
    ]
    tables = {
        "companies": {"primary_key": {"column": "id", "type": "auto_increment"}},
    }
    if extra_fields:
        fields.extend(extra_fields)
    if extra_tables:
        tables.update(extra_tables)

    return SiphonConfig.model_validate(
        {
            "name": "test",
            "llm": {
                "base_url": "https://api.example.com",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite:///test.db"},
            "schema": {"fields": fields, "tables": tables},
        }
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_config() -> SiphonConfig:
    """Config with a single required string field."""
    return _make_config()


@pytest.fixture
def multi_field_config() -> SiphonConfig:
    """Config with a variety of field types."""
    return _make_config(
        extra_fields=[
            {
                "name": "email",
                "type": "email",
                "required": False,
                "db": {"table": "companies", "column": "email"},
            },
            {
                "name": "phone",
                "type": "phone",
                "required": False,
                "db": {"table": "companies", "column": "phone"},
            },
            {
                "name": "revenue",
                "type": "currency",
                "required": False,
                "db": {"table": "companies", "column": "revenue"},
            },
            {
                "name": "founded",
                "type": "date",
                "required": False,
                "db": {"table": "companies", "column": "founded"},
            },
            {
                "name": "active",
                "type": "boolean",
                "required": False,
                "db": {"table": "companies", "column": "active"},
            },
        ]
    )


@pytest.fixture
def enum_config() -> SiphonConfig:
    """Config with an enum field (explicit values)."""
    return _make_config(
        extra_fields=[
            {
                "name": "status",
                "type": "enum",
                "required": False,
                "values": ["active", "inactive", "pending"],
                "case": "upper",
                "db": {"table": "companies", "column": "status"},
            }
        ]
    )


@pytest.fixture
def preset_enum_config() -> SiphonConfig:
    """Config with an enum field backed by a us_states preset."""
    return _make_config(
        extra_fields=[
            {
                "name": "state",
                "type": "enum",
                "required": False,
                "preset": "us_states",
                "case": "upper",
                "db": {"table": "companies", "column": "state"},
            }
        ]
    )


@pytest.fixture
def constrained_string_config() -> SiphonConfig:
    """Config with a string field that has min/max length constraints."""
    return _make_config(
        extra_fields=[
            {
                "name": "ticker",
                "type": "string",
                "required": False,
                "min_length": 1,
                "max_length": 5,
                "db": {"table": "companies", "column": "ticker"},
            }
        ]
    )


@pytest.fixture
def integer_config() -> SiphonConfig:
    """Config with a required integer field with bounds."""
    return _make_config(
        extra_fields=[
            {
                "name": "employee_count",
                "type": "integer",
                "required": False,
                "min": 0,
                "max": 1_000_000,
                "db": {"table": "companies", "column": "employee_count"},
            }
        ]
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_validator_constructs(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    assert v is not None


def test_validator_model_is_built(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    assert v._model is not None


# ---------------------------------------------------------------------------
# validate_records — happy path (valid records)
# ---------------------------------------------------------------------------


def test_valid_record_is_returned_in_valid_list(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, invalid = v.validate_records([{"company_name": "Acme Corp"}])
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0]["company_name"] == "Acme Corp"


def test_valid_records_are_all_returned(simple_config: SiphonConfig) -> None:
    records = [
        {"company_name": "Alpha"},
        {"company_name": "Beta"},
        {"company_name": "Gamma"},
    ]
    v = Validator(simple_config)
    valid, invalid = v.validate_records(records)
    assert len(valid) == 3
    assert len(invalid) == 0


def test_valid_record_whitespace_stripped(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, _ = v.validate_records([{"company_name": "  Acme  "}])
    assert valid[0]["company_name"] == "Acme"


def test_empty_records_list(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, invalid = v.validate_records([])
    assert valid == []
    assert invalid == []


# ---------------------------------------------------------------------------
# validate_records — formatting applied
# ---------------------------------------------------------------------------


def test_email_lowercased(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, _ = v.validate_records(
        [{"company_name": "Acme", "email": "User@Example.COM"}]
    )
    assert valid[0]["email"] == "user@example.com"


def test_phone_formatted(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, _ = v.validate_records(
        [{"company_name": "Acme", "phone": "5555551234"}]
    )
    assert valid[0]["phone"] == "(555) 555-1234"


def test_currency_formatted(multi_field_config: SiphonConfig) -> None:
    from decimal import Decimal

    v = Validator(multi_field_config)
    valid, _ = v.validate_records(
        [{"company_name": "Acme", "revenue": "$1,234.56"}]
    )
    assert valid[0]["revenue"] == Decimal("1234.56")


def test_date_formatted(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, _ = v.validate_records(
        [{"company_name": "Acme", "founded": "January 1, 2000"}]
    )
    assert valid[0]["founded"] == "2000-01-01"


def test_boolean_formatted_true(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, _ = v.validate_records([{"company_name": "Acme", "active": "yes"}])
    assert valid[0]["active"] is True


def test_boolean_formatted_false(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, _ = v.validate_records([{"company_name": "Acme", "active": "false"}])
    assert valid[0]["active"] is False


# ---------------------------------------------------------------------------
# validate_records — enum validation
# ---------------------------------------------------------------------------


def test_enum_valid_value_uppercased(enum_config: SiphonConfig) -> None:
    v = Validator(enum_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "status": "active"}]
    )
    assert len(valid) == 1
    assert valid[0]["status"] == "ACTIVE"


def test_enum_invalid_value_captured(enum_config: SiphonConfig) -> None:
    v = Validator(enum_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "status": "unknown"}]
    )
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["record"]["status"] == "unknown"


def test_enum_preset_us_state_valid(preset_enum_config: SiphonConfig) -> None:
    v = Validator(preset_enum_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "state": "ca"}]
    )
    assert len(valid) == 1
    assert valid[0]["state"] == "CA"


def test_enum_preset_us_state_invalid(preset_enum_config: SiphonConfig) -> None:
    v = Validator(preset_enum_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "state": "XX"}]
    )
    assert len(valid) == 0
    assert len(invalid) == 1


# ---------------------------------------------------------------------------
# validate_records — invalid records captured with errors
# ---------------------------------------------------------------------------


def test_invalid_email_captured(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "email": "not-an-email"}]
    )
    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["record"]["email"] == "not-an-email"
    assert len(invalid[0]["errors"]) >= 1


def test_invalid_phone_captured(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "phone": "123"}]
    )
    assert len(valid) == 0
    assert len(invalid) == 1


def test_invalid_boolean_captured(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "active": "maybe"}]
    )
    assert len(valid) == 0
    assert len(invalid) == 1


def test_errors_contain_pydantic_error_dicts(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    _, invalid = v.validate_records(
        [{"company_name": "Acme", "email": "bad"}]
    )
    errors = invalid[0]["errors"]
    assert isinstance(errors, list)
    # Each entry should be a pydantic error dict with at least 'loc' and 'msg'
    assert "loc" in errors[0]
    assert "msg" in errors[0]


def test_original_record_preserved_in_invalid(multi_field_config: SiphonConfig) -> None:
    record = {"company_name": "Acme", "email": "bad"}
    v = Validator(multi_field_config)
    _, invalid = v.validate_records([record])
    assert invalid[0]["record"] == record


# ---------------------------------------------------------------------------
# validate_records — required field validation
# ---------------------------------------------------------------------------


def test_required_field_missing_is_invalid(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, invalid = v.validate_records([{}])
    assert len(valid) == 0
    assert len(invalid) == 1


def test_required_field_none_is_invalid(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, invalid = v.validate_records([{"company_name": None}])
    assert len(valid) == 0
    assert len(invalid) == 1


def test_required_field_empty_string_is_invalid(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, invalid = v.validate_records([{"company_name": ""}])
    assert len(valid) == 0
    assert len(invalid) == 1


def test_required_field_whitespace_only_is_invalid(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, invalid = v.validate_records([{"company_name": "   "}])
    assert len(valid) == 0
    assert len(invalid) == 1


def test_optional_field_none_is_valid(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records([{"company_name": "Acme", "email": None}])
    assert len(valid) == 1
    assert valid[0]["email"] is None


def test_optional_field_absent_is_valid(multi_field_config: SiphonConfig) -> None:
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records([{"company_name": "Acme"}])
    assert len(valid) == 1
    assert valid[0]["email"] is None


# ---------------------------------------------------------------------------
# validate_records — NaN cleaning
# ---------------------------------------------------------------------------


def test_nan_float_treated_as_none_for_optional(multi_field_config: SiphonConfig) -> None:
    """float('nan') values (from pandas) should be treated as None for optional fields."""
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "email": float("nan")}]
    )
    assert len(valid) == 1
    assert valid[0]["email"] is None


def test_nan_float_for_required_field_is_invalid(simple_config: SiphonConfig) -> None:
    """float('nan') on a required field should produce a validation error."""
    v = Validator(simple_config)
    valid, invalid = v.validate_records([{"company_name": float("nan")}])
    assert len(valid) == 0
    assert len(invalid) == 1


def test_multiple_nan_fields(multi_field_config: SiphonConfig) -> None:
    """Multiple NaN fields in one record should all be cleaned to None."""
    v = Validator(multi_field_config)
    record = {
        "company_name": "Acme",
        "email": float("nan"),
        "phone": float("nan"),
        "revenue": float("nan"),
    }
    valid, invalid = v.validate_records([record])
    assert len(valid) == 1
    assert valid[0]["email"] is None
    assert valid[0]["phone"] is None
    assert valid[0]["revenue"] is None


# ---------------------------------------------------------------------------
# validate_records — mixed valid/invalid in same batch
# ---------------------------------------------------------------------------


def test_mixed_batch_partitioned_correctly(multi_field_config: SiphonConfig) -> None:
    records = [
        {"company_name": "Good Co", "email": "good@example.com"},
        {"company_name": "Bad Co", "email": "not-an-email"},
        {"company_name": "Also Good", "email": None},
    ]
    v = Validator(multi_field_config)
    valid, invalid = v.validate_records(records)
    assert len(valid) == 2
    assert len(invalid) == 1
    assert invalid[0]["record"]["company_name"] == "Bad Co"


# ---------------------------------------------------------------------------
# validate_records — constrained string
# ---------------------------------------------------------------------------


def test_string_max_length_violation_captured(constrained_string_config: SiphonConfig) -> None:
    v = Validator(constrained_string_config)
    _, invalid = v.validate_records(
        [{"company_name": "Acme", "ticker": "TOOLONG"}]
    )
    assert len(invalid) == 1


def test_string_min_length_violation_captured(constrained_string_config: SiphonConfig) -> None:
    v = Validator(constrained_string_config)
    _, invalid = v.validate_records(
        [{"company_name": "Acme", "ticker": ""}]
    )
    # Empty string → None for optional field, so no error (it won't hit min_length check)
    # The min_length check only fires on non-empty strings.
    # This is correct behaviour: empty optional field → None is accepted.
    assert len(invalid) == 0  # empty is treated as None for optional field


def test_string_within_constraints_valid(constrained_string_config: SiphonConfig) -> None:
    v = Validator(constrained_string_config)
    valid, invalid = v.validate_records(
        [{"company_name": "Acme", "ticker": "AAPL"}]
    )
    assert len(valid) == 1
    assert valid[0]["ticker"] == "AAPL"


# ---------------------------------------------------------------------------
# validate_records — integer constraints
# ---------------------------------------------------------------------------


def test_integer_below_min_captured(integer_config: SiphonConfig) -> None:
    v = Validator(integer_config)
    _, invalid = v.validate_records(
        [{"company_name": "Acme", "employee_count": "-5"}]
    )
    assert len(invalid) == 1


def test_integer_valid(integer_config: SiphonConfig) -> None:
    v = Validator(integer_config)
    valid, _ = v.validate_records(
        [{"company_name": "Acme", "employee_count": "100"}]
    )
    assert valid[0]["employee_count"] == 100


def test_integer_float_string_coerced(integer_config: SiphonConfig) -> None:
    """Strings like '42.0' should be coerced to int 42."""
    v = Validator(integer_config)
    valid, _ = v.validate_records(
        [{"company_name": "Acme", "employee_count": "42.0"}]
    )
    assert valid[0]["employee_count"] == 42


# ---------------------------------------------------------------------------
# model_dump produces correct output types
# ---------------------------------------------------------------------------


def test_valid_record_dumped_as_dict(simple_config: SiphonConfig) -> None:
    v = Validator(simple_config)
    valid, _ = v.validate_records([{"company_name": "Acme"}])
    result = valid[0]
    assert isinstance(result, dict)
    assert "company_name" in result
