"""Tests for the Validator.deduplicate() method and related helpers."""

from __future__ import annotations

import copy

import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.validator import Validator


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(dedup: dict | None = None, extra_fields: list[dict] | None = None) -> SiphonConfig:
    """Build a SiphonConfig with optional deduplication config and extra fields."""
    fields = [
        {
            "name": "company_name",
            "type": "string",
            "required": True,
            "db": {"table": "companies", "column": "name"},
        },
        {
            "name": "state",
            "type": "string",
            "required": False,
            "db": {"table": "companies", "column": "state"},
        },
    ]
    if extra_fields:
        fields.extend(extra_fields)

    schema: dict = {
        "fields": fields,
        "tables": {
            "companies": {"primary_key": {"column": "id", "type": "auto_increment"}},
        },
    }
    if dedup is not None:
        schema["deduplication"] = dedup

    return SiphonConfig.model_validate(
        {
            "name": "test",
            "llm": {
                "base_url": "https://api.example.com",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite:///test.db"},
            "schema": schema,
        }
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_config_dict():
    """Minimal valid SiphonConfig as a dict (mirrors the shared conftest fixture)."""
    return {
        "name": "test_pipeline",
        "llm": {
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "api_key": "sk-test-key",
        },
        "database": {
            "url": "sqlite+aiosqlite:///test.db",
        },
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
                {
                    "name": "state",
                    "type": "string",
                    "required": False,
                    "db": {"table": "companies", "column": "state"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "pipeline": {
            "chunk_size": 50,
            "review": False,
            "log_level": "info",
        },
    }


@pytest.fixture
def config_no_dedup(sample_config_dict) -> SiphonConfig:
    """Config with no deduplication section."""
    return SiphonConfig.model_validate(sample_config_dict)


@pytest.fixture
def config_exact(sample_config_dict) -> SiphonConfig:
    """Config with exact single-field deduplication."""
    d = copy.deepcopy(sample_config_dict)
    d["schema"]["deduplication"] = {"key": ["company_name"], "match": "exact"}
    return SiphonConfig.model_validate(d)


@pytest.fixture
def config_case_insensitive(sample_config_dict) -> SiphonConfig:
    """Config with case-insensitive single-field deduplication."""
    d = copy.deepcopy(sample_config_dict)
    d["schema"]["deduplication"] = {"key": ["company_name"], "match": "case_insensitive"}
    return SiphonConfig.model_validate(d)


@pytest.fixture
def config_composite(sample_config_dict) -> SiphonConfig:
    """Config with composite key deduplication on [company_name, state]."""
    d = copy.deepcopy(sample_config_dict)
    d["schema"]["deduplication"] = {"key": ["company_name", "state"], "match": "exact"}
    return SiphonConfig.model_validate(d)


@pytest.fixture
def config_composite_ci(sample_config_dict) -> SiphonConfig:
    """Config with composite key, case-insensitive deduplication."""
    d = copy.deepcopy(sample_config_dict)
    d["schema"]["deduplication"] = {
        "key": ["company_name", "state"],
        "match": "case_insensitive",
    }
    return SiphonConfig.model_validate(d)


# ---------------------------------------------------------------------------
# 1. No dedup config → all records returned, no duplicates
# ---------------------------------------------------------------------------


def test_no_dedup_config_returns_all(config_no_dedup: SiphonConfig) -> None:
    v = Validator(config_no_dedup)
    records = [
        {"company_name": "Acme"},
        {"company_name": "Acme"},
        {"company_name": "Beta"},
    ]
    unique, dupes = v.deduplicate(records)
    assert unique == records
    assert dupes == []


# ---------------------------------------------------------------------------
# 2. Batch dedup exact match: duplicate detected and removed
# ---------------------------------------------------------------------------


def test_exact_dedup_removes_duplicate(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    records = [
        {"company_name": "Acme"},
        {"company_name": "Beta"},
        {"company_name": "Acme"},  # duplicate
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 2
    assert len(dupes) == 1
    assert unique[0]["company_name"] == "Acme"
    assert unique[1]["company_name"] == "Beta"
    assert dupes[0]["company_name"] == "Acme"


def test_exact_dedup_no_duplicates_returns_all(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    records = [
        {"company_name": "Alpha"},
        {"company_name": "Beta"},
        {"company_name": "Gamma"},
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 3
    assert dupes == []


# ---------------------------------------------------------------------------
# 3. Batch dedup case_insensitive: "Acme" and "acme" are duplicates
# ---------------------------------------------------------------------------


def test_case_insensitive_treats_different_cases_as_duplicate(
    config_case_insensitive: SiphonConfig,
) -> None:
    v = Validator(config_case_insensitive)
    records = [
        {"company_name": "Acme"},
        {"company_name": "acme"},   # duplicate (case-insensitive)
        {"company_name": "ACME"},   # duplicate
        {"company_name": "Beta"},
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 2
    assert len(dupes) == 2
    assert unique[0]["company_name"] == "Acme"
    assert unique[1]["company_name"] == "Beta"


# ---------------------------------------------------------------------------
# 4. Batch dedup exact match: "Acme" and "acme" are NOT duplicates
# ---------------------------------------------------------------------------


def test_exact_treats_different_cases_as_unique(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    records = [
        {"company_name": "Acme"},
        {"company_name": "acme"},
        {"company_name": "ACME"},
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 3
    assert dupes == []


# ---------------------------------------------------------------------------
# 5. Composite key: same name but different state → NOT a duplicate
# ---------------------------------------------------------------------------


def test_composite_key_different_state_is_unique(config_composite: SiphonConfig) -> None:
    v = Validator(config_composite)
    records = [
        {"company_name": "Acme", "state": "CA"},
        {"company_name": "Acme", "state": "NY"},  # different state → unique
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 2
    assert dupes == []


# ---------------------------------------------------------------------------
# 6. Composite key: same values on both fields → duplicate
# ---------------------------------------------------------------------------


def test_composite_key_same_values_is_duplicate(config_composite: SiphonConfig) -> None:
    v = Validator(config_composite)
    records = [
        {"company_name": "Acme", "state": "CA"},
        {"company_name": "Acme", "state": "CA"},  # exact duplicate
        {"company_name": "Beta", "state": "TX"},
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 2
    assert len(dupes) == 1
    assert dupes[0]["company_name"] == "Acme"
    assert dupes[0]["state"] == "CA"


# ---------------------------------------------------------------------------
# 7. Pre-existing DB keys: record matching existing key is skipped
# ---------------------------------------------------------------------------


def test_existing_db_keys_skips_matching_record(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    existing_keys = {("Acme",)}
    records = [
        {"company_name": "Acme"},  # exists in DB → duplicate
        {"company_name": "Beta"},  # new → unique
    ]
    unique, dupes = v.deduplicate(records, existing_keys=existing_keys)
    assert len(unique) == 1
    assert len(dupes) == 1
    assert unique[0]["company_name"] == "Beta"
    assert dupes[0]["company_name"] == "Acme"


# ---------------------------------------------------------------------------
# 8. Pre-existing DB keys + batch dedup combined
# ---------------------------------------------------------------------------


def test_db_keys_and_batch_dedup_combined(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    existing_keys = {("ExistingCo",)}
    records = [
        {"company_name": "ExistingCo"},  # matches DB → duplicate
        {"company_name": "NewCo"},       # new
        {"company_name": "NewCo"},       # batch duplicate of previous
        {"company_name": "AnotherNew"},  # unique
    ]
    unique, dupes = v.deduplicate(records, existing_keys=existing_keys)
    assert len(unique) == 2
    assert len(dupes) == 2
    names = [r["company_name"] for r in unique]
    assert "NewCo" in names
    assert "AnotherNew" in names


# ---------------------------------------------------------------------------
# 9. None/empty values in dedup key fields handled gracefully
# ---------------------------------------------------------------------------


def test_none_value_in_key_field_treated_as_empty_string(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    records = [
        {"company_name": None},
        {"company_name": None},  # duplicate (both become "")
        {"company_name": "Real"},
    ]
    unique, dupes = v.deduplicate(records)
    assert len(unique) == 2
    assert len(dupes) == 1


def test_missing_key_field_treated_as_empty_string(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    records = [
        {},                        # missing company_name → ""
        {"company_name": None},    # None → ""
        {"company_name": "Real"},
    ]
    unique, dupes = v.deduplicate(records)
    # Both None and missing are treated as "" → only one unique "" record
    assert len(unique) == 2
    assert len(dupes) == 1


# ---------------------------------------------------------------------------
# 10. build_existing_keys static method works correctly
# ---------------------------------------------------------------------------


def test_build_existing_keys_basic() -> None:
    rows = [
        {"company_name": "Acme", "state": "CA"},
        {"company_name": "Beta", "state": "NY"},
    ]
    keys = Validator.build_existing_keys(rows, ["company_name"], case_insensitive=False)
    assert keys == {("Acme",), ("Beta",)}


def test_build_existing_keys_composite() -> None:
    rows = [
        {"company_name": "Acme", "state": "CA"},
        {"company_name": "Acme", "state": "NY"},
    ]
    keys = Validator.build_existing_keys(rows, ["company_name", "state"], case_insensitive=False)
    assert keys == {("Acme", "CA"), ("Acme", "NY")}


def test_build_existing_keys_none_value_becomes_empty_string() -> None:
    rows = [{"company_name": None}]
    keys = Validator.build_existing_keys(rows, ["company_name"], case_insensitive=False)
    assert keys == {("",)}


def test_build_existing_keys_missing_field_becomes_empty_string() -> None:
    rows = [{"other_field": "x"}]
    keys = Validator.build_existing_keys(rows, ["company_name"], case_insensitive=False)
    assert keys == {("",)}


def test_build_existing_keys_deduplicates_rows() -> None:
    rows = [
        {"company_name": "Acme"},
        {"company_name": "Acme"},  # same row → set deduplicates
    ]
    keys = Validator.build_existing_keys(rows, ["company_name"], case_insensitive=False)
    assert keys == {("Acme",)}


# ---------------------------------------------------------------------------
# 11. Case-insensitive build_existing_keys
# ---------------------------------------------------------------------------


def test_build_existing_keys_case_insensitive_lowercases() -> None:
    rows = [
        {"company_name": "ACME"},
        {"company_name": "Acme"},
        {"company_name": "acme"},
    ]
    keys = Validator.build_existing_keys(rows, ["company_name"], case_insensitive=True)
    # All three lowercase to "acme" → one key in the set
    assert keys == {("acme",)}


def test_build_existing_keys_case_insensitive_composite() -> None:
    rows = [
        {"company_name": "ACME", "state": "CA"},
        {"company_name": "Beta", "state": "NY"},
    ]
    keys = Validator.build_existing_keys(
        rows, ["company_name", "state"], case_insensitive=True
    )
    assert ("acme", "ca") in keys
    assert ("beta", "ny") in keys


def test_case_insensitive_existing_keys_used_for_dedup(
    config_case_insensitive: SiphonConfig,
) -> None:
    """DB keys built with case_insensitive=True are correctly matched in deduplicate()."""
    v = Validator(config_case_insensitive)
    # Simulate DB row stored as "ACME"
    existing_keys = Validator.build_existing_keys(
        [{"company_name": "ACME"}], ["company_name"], case_insensitive=True
    )
    records = [
        {"company_name": "acme"},   # should match DB (case-insensitive) → duplicate
        {"company_name": "Beta"},   # new
    ]
    unique, dupes = v.deduplicate(records, existing_keys=existing_keys)
    assert len(unique) == 1
    assert len(dupes) == 1
    assert unique[0]["company_name"] == "Beta"
    assert dupes[0]["company_name"] == "acme"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_records_list_returns_empty(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    unique, dupes = v.deduplicate([])
    assert unique == []
    assert dupes == []


def test_single_record_always_unique(config_exact: SiphonConfig) -> None:
    v = Validator(config_exact)
    unique, dupes = v.deduplicate([{"company_name": "Acme"}])
    assert len(unique) == 1
    assert dupes == []


def test_first_occurrence_kept_not_second(config_exact: SiphonConfig) -> None:
    """The first record seen should be kept; subsequent duplicates go to dupes."""
    v = Validator(config_exact)
    rec1 = {"company_name": "Acme", "extra": "first"}
    rec2 = {"company_name": "Acme", "extra": "second"}
    unique, dupes = v.deduplicate([rec1, rec2])
    assert unique[0] is rec1
    assert dupes[0] is rec2
