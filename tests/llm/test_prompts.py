"""Tests for siphon/llm/prompts.py — dynamic prompt generation from schema config."""

from __future__ import annotations

import pytest

from siphon.config.schema import FieldConfig, FieldDBConfig
from siphon.llm.prompts import (
    _field_description,
    _format_enum_values,
    build_correction_prompt,
    build_extraction_prompt,
    build_revision_prompt,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_field(
    name: str,
    type_: str,
    *,
    required: bool = False,
    values: list[str] | None = None,
    preset: str | None = None,
    country_code: str | None = None,
) -> FieldConfig:
    """Construct a minimal FieldConfig for testing."""
    return FieldConfig(
        name=name,
        type=type_,  # type: ignore[arg-type]
        db=FieldDBConfig(table="companies", column=name),
        required=required,
        values=values,
        preset=preset,
        country_code=country_code,
    )


SIMPLE_CSV = "name,phone\nAcme,555-1234\nFoo Inc,555-5678\n"

# ---------------------------------------------------------------------------
# _format_enum_values
# ---------------------------------------------------------------------------


class TestFormatEnumValues:
    def test_few_values_shows_all(self):
        values = ["A", "B", "C"]
        result = _format_enum_values(values)
        assert result == "A, B, C"

    def test_exactly_ten_values_shows_all(self):
        values = [str(i) for i in range(10)]
        result = _format_enum_values(values)
        assert "..." not in result
        assert all(v in result for v in values)

    def test_eleven_values_truncates(self):
        values = [str(i) for i in range(11)]
        result = _format_enum_values(values)
        assert "..." in result

    def test_large_list_shows_first_five_and_last_two(self):
        # Simulate US-style state codes — use 54 entries
        values = [f"S{i:02d}" for i in range(54)]
        result = _format_enum_values(values)
        # First 5 must appear
        for v in values[:5]:
            assert v in result
        # Last 2 must appear
        for v in values[-2:]:
            assert v in result
        # A value from the middle should NOT appear individually
        # (we just check the ellipsis is present)
        assert "..." in result

    def test_empty_list_returns_empty_string(self):
        assert _format_enum_values([]) == ""

    def test_single_value(self):
        assert _format_enum_values(["ONLY"]) == "ONLY"


# ---------------------------------------------------------------------------
# _field_description
# ---------------------------------------------------------------------------


class TestFieldDescription:
    def test_string_field(self):
        f = _make_field("company_name", "string")
        desc = _field_description(f)
        assert desc == "company_name (string)"

    def test_string_required(self):
        f = _make_field("company_name", "string", required=True)
        desc = _field_description(f)
        assert desc == "company_name (string, required)"

    def test_phone_field(self):
        f = _make_field("phone", "phone")
        desc = _field_description(f)
        assert desc == "phone (phone number)"

    def test_url_field(self):
        f = _make_field("website", "url")
        desc = _field_description(f)
        assert desc == "website (url)"

    def test_country_field(self):
        f = _make_field("country", "country")
        desc = _field_description(f)
        assert desc == "country (ISO 3166-1 country code)"

    def test_subdivision_field_with_country_code(self):
        f = _make_field("state", "subdivision", country_code="US")
        desc = _field_description(f)
        assert desc == "state (subdivision, country: US)"

    def test_subdivision_field_no_country_code(self):
        f = _make_field("province", "subdivision")
        desc = _field_description(f)
        assert "subdivision" in desc
        assert "country" in desc

    def test_enum_with_explicit_values_few(self):
        f = _make_field("status", "enum", values=["active", "inactive", "pending"])
        desc = _field_description(f)
        assert "enum:" in desc
        assert "active" in desc
        assert "inactive" in desc
        assert "pending" in desc

    def test_enum_with_explicit_values_many_truncates(self):
        values = [f"V{i:02d}" for i in range(15)]
        f = _make_field("code", "enum", values=values)
        desc = _field_description(f)
        assert "enum:" in desc
        assert "..." in desc
        # First 5 visible
        for v in values[:5]:
            assert v in desc
        # Last 2 visible
        for v in values[-2:]:
            assert v in desc

    def test_enum_with_preset_us_states(self):
        f = _make_field("state", "enum", preset="us_states")
        desc = _field_description(f)
        assert "enum:" in desc
        # US states list is >10, so should be truncated with "..."
        assert "..." in desc
        # Well-known codes from the head of the sorted list should appear
        # (AK is the first alphabetically)
        assert "AK" in desc

    def test_enum_with_preset_ca_provinces(self):
        f = _make_field("province", "enum", preset="ca_provinces")
        desc = _field_description(f)
        assert "enum:" in desc

    def test_enum_no_values_no_preset(self):
        f = _make_field("category", "enum")
        desc = _field_description(f)
        assert "enum" in desc

    def test_required_appended_after_type_info(self):
        f = _make_field("state", "enum", values=["CA", "TX"], required=True)
        desc = _field_description(f)
        assert desc.endswith(", required)")

    def test_boolean_field(self):
        f = _make_field("active", "boolean")
        desc = _field_description(f)
        assert desc == "active (boolean)"

    def test_integer_field(self):
        f = _make_field("headcount", "integer")
        desc = _field_description(f)
        assert desc == "headcount (integer)"

    def test_email_field(self):
        f = _make_field("email", "email")
        desc = _field_description(f)
        assert desc == "email (email)"

    def test_date_field(self):
        f = _make_field("founded", "date")
        desc = _field_description(f)
        assert desc == "founded (date)"


# ---------------------------------------------------------------------------
# build_extraction_prompt
# ---------------------------------------------------------------------------


class TestBuildExtractionPrompt:
    def _default_fields(self) -> list[FieldConfig]:
        return [
            _make_field("company_name", "string", required=True),
            _make_field("phone", "phone"),
            _make_field("website", "url"),
            _make_field("state", "enum", preset="us_states"),
        ]

    def test_returns_string(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert isinstance(prompt, str)

    def test_contains_all_field_names(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        for name in ("company_name", "phone", "website", "state"):
            assert name in prompt

    def test_contains_field_types(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "string" in prompt
        assert "phone number" in prompt
        assert "url" in prompt
        assert "enum:" in prompt

    def test_required_field_marked_required(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "company_name (string, required)" in prompt

    def test_row_count_in_prompt(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=42,
        )
        assert "42" in prompt

    def test_csv_data_included(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert SIMPLE_CSV in prompt

    def test_extraction_hints_included_when_provided(self):
        hints = "Normalize all phone numbers to E.164 format."
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
            extraction_hints=hints,
        )
        assert hints in prompt

    def test_extraction_hints_omitted_when_none(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
            extraction_hints=None,
        )
        assert "Additional instructions:" not in prompt

    def test_extraction_hints_omitted_when_not_passed(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "Additional instructions:" not in prompt

    def test_json_array_rule_present(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=3,
        )
        assert "JSON array" in prompt
        assert "3" in prompt

    def test_empty_string_rule_present(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert '""' in prompt

    def test_enum_field_with_explicit_values(self):
        fields = [_make_field("status", "enum", values=["active", "inactive"])]
        prompt = build_extraction_prompt(
            fields=fields,
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "active" in prompt
        assert "inactive" in prompt

    def test_subdivision_field_shows_country(self):
        fields = [_make_field("state", "subdivision", country_code="US")]
        prompt = build_extraction_prompt(
            fields=fields,
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "subdivision" in prompt
        assert "US" in prompt

    def test_country_field_shows_iso_description(self):
        fields = [_make_field("country", "country")]
        prompt = build_extraction_prompt(
            fields=fields,
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "ISO 3166-1" in prompt

    def test_single_field_prompt(self):
        fields = [_make_field("name", "string", required=True)]
        prompt = build_extraction_prompt(
            fields=fields,
            chunk_csv="name\nAcme\n",
            row_count=1,
        )
        assert "name (string, required)" in prompt
        assert "1" in prompt

    def test_prompt_structure_has_fields_section(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "Fields to extract:" in prompt

    def test_prompt_structure_has_rules_section(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "Rules:" in prompt

    def test_prompt_structure_has_csv_section(self):
        prompt = build_extraction_prompt(
            fields=self._default_fields(),
            chunk_csv=SIMPLE_CSV,
            row_count=2,
        )
        assert "CSV data:" in prompt


# ---------------------------------------------------------------------------
# build_revision_prompt
# ---------------------------------------------------------------------------


class TestBuildRevisionPrompt:
    SAMPLE_JSON = '[{"company_name": "Acme", "state": "CA"}]'
    SAMPLE_COMMAND = "Capitalize all company names"

    def test_returns_string(self):
        result = build_revision_prompt(self.SAMPLE_JSON, self.SAMPLE_COMMAND)
        assert isinstance(result, str)

    def test_batch_json_included(self):
        result = build_revision_prompt(self.SAMPLE_JSON, self.SAMPLE_COMMAND)
        assert self.SAMPLE_JSON in result

    def test_command_included(self):
        result = build_revision_prompt(self.SAMPLE_JSON, self.SAMPLE_COMMAND)
        assert self.SAMPLE_COMMAND in result

    def test_command_is_quoted(self):
        result = build_revision_prompt(self.SAMPLE_JSON, self.SAMPLE_COMMAND)
        assert f'"{self.SAMPLE_COMMAND}"' in result

    def test_instructs_return_json_array(self):
        result = build_revision_prompt(self.SAMPLE_JSON, self.SAMPLE_COMMAND)
        assert "JSON array" in result

    def test_mentions_same_structure(self):
        result = build_revision_prompt(self.SAMPLE_JSON, self.SAMPLE_COMMAND)
        assert "same structure" in result

    def test_different_command_changes_prompt(self):
        cmd1 = build_revision_prompt(self.SAMPLE_JSON, "Command A")
        cmd2 = build_revision_prompt(self.SAMPLE_JSON, "Command B")
        assert cmd1 != cmd2

    def test_different_json_changes_prompt(self):
        p1 = build_revision_prompt('["a"]', self.SAMPLE_COMMAND)
        p2 = build_revision_prompt('["b"]', self.SAMPLE_COMMAND)
        assert p1 != p2

    def test_empty_command(self):
        result = build_revision_prompt(self.SAMPLE_JSON, "")
        assert self.SAMPLE_JSON in result
        # Should still produce a valid string
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# build_correction_prompt
# ---------------------------------------------------------------------------


class TestBuildCorrectionPrompt:
    def test_returns_string(self):
        result = build_correction_prompt(expected=10, actual=8)
        assert isinstance(result, str)

    def test_actual_count_in_prompt(self):
        result = build_correction_prompt(expected=10, actual=8)
        assert "8" in result

    def test_expected_count_in_prompt(self):
        result = build_correction_prompt(expected=10, actual=8)
        assert "10" in result

    def test_expected_count_appears_twice(self):
        """The expected count should appear in both the description and the instruction."""
        result = build_correction_prompt(expected=25, actual=20)
        assert result.count("25") >= 2

    def test_prompt_says_return_exactly(self):
        result = build_correction_prompt(expected=10, actual=8)
        assert "Return exactly" in result or "return exactly" in result.lower()

    def test_different_values_produce_different_prompts(self):
        r1 = build_correction_prompt(expected=10, actual=8)
        r2 = build_correction_prompt(expected=10, actual=5)
        assert r1 != r2

    def test_matches_spec_format(self):
        result = build_correction_prompt(expected=5, actual=3)
        assert "3" in result
        assert "5" in result
        assert "5 objects" in result

    def test_zero_actual(self):
        result = build_correction_prompt(expected=10, actual=0)
        assert "0" in result
        assert "10" in result

    def test_more_actual_than_expected(self):
        """Works when LLM returns too many rows."""
        result = build_correction_prompt(expected=5, actual=8)
        assert "8" in result
        assert "5" in result
