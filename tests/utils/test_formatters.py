"""Comprehensive tests for siphon.utils.formatters — all 14 field type formatters."""

from decimal import Decimal

import pytest

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
# Shared empty-value parametrisation
# ---------------------------------------------------------------------------

_EMPTY_VALUES = [None, "", "   ", "\t", "\n"]


# ---------------------------------------------------------------------------
# format_string
# ---------------------------------------------------------------------------


class TestFormatString:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_string(empty) is None

    def test_strips_leading_trailing_whitespace(self):
        assert format_string("  Acme  ") == "Acme"

    def test_plain_string_unchanged(self):
        assert format_string("Hello") == "Hello"

    def test_non_string_converted(self):
        assert format_string(123) == "123"

    def test_min_length_passes(self):
        assert format_string("Hi", min_length=2) == "Hi"

    def test_min_length_fails(self):
        with pytest.raises(ValueError, match="min_length"):
            format_string("Hi", min_length=5)

    def test_max_length_passes(self):
        assert format_string("Hello", max_length=10) == "Hello"

    def test_max_length_fails(self):
        with pytest.raises(ValueError, match="max_length"):
            format_string("Hello World", max_length=5)

    def test_exact_min_length_boundary(self):
        assert format_string("AB", min_length=2) == "AB"

    def test_exact_max_length_boundary(self):
        assert format_string("ABCDE", max_length=5) == "ABCDE"

    def test_internal_whitespace_preserved(self):
        assert format_string("  Hello World  ") == "Hello World"


# ---------------------------------------------------------------------------
# format_integer
# ---------------------------------------------------------------------------


class TestFormatInteger:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_integer(empty) is None

    def test_string_digit(self):
        assert format_integer("42") == 42

    def test_native_int(self):
        assert format_integer(7) == 7

    def test_float_string_truncates(self):
        assert format_integer("42.9") == 42

    def test_negative(self):
        assert format_integer("-5") == -5

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError, match="integer"):
            format_integer("not_a_number")

    def test_min_passes(self):
        assert format_integer("10", min=5) == 10

    def test_min_fails(self):
        with pytest.raises(ValueError, match="min="):
            format_integer("3", min=5)

    def test_max_passes(self):
        assert format_integer("10", max=20) == 10

    def test_max_fails(self):
        with pytest.raises(ValueError, match="max="):
            format_integer("25", max=20)

    def test_zero(self):
        assert format_integer("0") == 0

    def test_whitespace_stripped(self):
        assert format_integer("  8  ") == 8


# ---------------------------------------------------------------------------
# format_number
# ---------------------------------------------------------------------------


class TestFormatNumber:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_number(empty) is None

    def test_float_string(self):
        assert format_number("42.5") == 42.5

    def test_integer_string(self):
        assert format_number("10") == 10.0

    def test_native_float(self):
        assert format_number(3.14) == 3.14

    def test_negative(self):
        assert format_number("-1.5") == -1.5

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="number"):
            format_number("abc")

    def test_min_passes(self):
        assert format_number("5.0", min=0.0) == 5.0

    def test_min_fails(self):
        with pytest.raises(ValueError, match="min="):
            format_number("-1.0", min=0.0)

    def test_max_passes(self):
        assert format_number("99.9", max=100.0) == 99.9

    def test_max_fails(self):
        with pytest.raises(ValueError, match="max="):
            format_number("101.0", max=100.0)

    def test_scientific_notation(self):
        assert format_number("1e3") == 1000.0


# ---------------------------------------------------------------------------
# format_currency
# ---------------------------------------------------------------------------


class TestFormatCurrency:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_currency(empty) is None

    def test_dollar_sign_comma(self):
        assert format_currency("$1,234.56") == Decimal("1234.56")

    def test_plain_number(self):
        assert format_currency("9.99") == Decimal("9.99")

    def test_parenthetical_negative(self):
        assert format_currency("(123.45)") == Decimal("-123.45")

    def test_leading_minus(self):
        assert format_currency("-50.00") == Decimal("-50.00")

    def test_negative_with_dollar_sign(self):
        assert format_currency("-$10.00") == Decimal("-10.00")

    def test_rounded_to_two_places(self):
        # 1.006 unambiguously rounds up to 1.01 in Decimal arithmetic
        assert format_currency("1.006") == Decimal("1.01")

    def test_whole_number(self):
        assert format_currency("100") == Decimal("100.00")

    def test_invalid_raises(self):
        with pytest.raises(ValueError, match="currency"):
            format_currency("not_money")

    def test_returns_decimal_type(self):
        result = format_currency("5.00")
        assert isinstance(result, Decimal)

    def test_large_number(self):
        assert format_currency("$1,000,000.00") == Decimal("1000000.00")


# ---------------------------------------------------------------------------
# format_phone
# ---------------------------------------------------------------------------


class TestFormatPhone:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_phone(empty) is None

    def test_ten_digit_string(self):
        assert format_phone("5551234567") == "(555) 123-4567"

    def test_eleven_digit_with_country_code(self):
        assert format_phone("15551234567") == "(555) 123-4567"

    def test_formatted_input_with_dashes(self):
        assert format_phone("555-123-4567") == "(555) 123-4567"

    def test_formatted_input_with_parens(self):
        assert format_phone("(555) 123-4567") == "(555) 123-4567"

    def test_dots_separator(self):
        assert format_phone("555.123.4567") == "(555) 123-4567"

    def test_eleven_digits_wrong_country_code_raises(self):
        with pytest.raises(ValueError, match="country code 1"):
            format_phone("25551234567")

    def test_too_few_digits_raises(self):
        with pytest.raises(ValueError, match="10 or 11 digits"):
            format_phone("12345")

    def test_too_many_digits_raises(self):
        with pytest.raises(ValueError, match="10 or 11 digits"):
            format_phone("123456789012")

    def test_output_format(self):
        result = format_phone("8005551234")
        assert result == "(800) 555-1234"


# ---------------------------------------------------------------------------
# format_url
# ---------------------------------------------------------------------------


class TestFormatUrl:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_url(empty) is None

    def test_prepends_http_if_missing(self):
        assert format_url("acme.com") == "http://acme.com"

    def test_preserves_https_scheme(self):
        assert format_url("https://acme.com") == "https://acme.com"

    def test_preserves_http_scheme(self):
        assert format_url("http://example.org") == "http://example.org"

    def test_url_with_path(self):
        assert format_url("example.com/path/to/page") == "http://example.com/path/to/page"

    def test_url_with_query(self):
        result = format_url("example.com?q=1")
        assert result == "http://example.com?q=1"

    def test_invalid_url_raises(self):
        with pytest.raises(ValueError, match="Invalid URL"):
            format_url("not a url at all")

    def test_ftp_scheme_preserved(self):
        assert format_url("ftp://files.example.com") == "ftp://files.example.com"

    def test_whitespace_stripped(self):
        assert format_url("  acme.com  ") == "http://acme.com"


# ---------------------------------------------------------------------------
# format_email
# ---------------------------------------------------------------------------


class TestFormatEmail:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_email(empty) is None

    def test_lowercases_email(self):
        assert format_email("Bob@Acme.COM") == "bob@acme.com"

    def test_already_lowercase(self):
        assert format_email("user@example.com") == "user@example.com"

    def test_mixed_case_local_and_domain(self):
        assert format_email("Alice.Smith@Company.ORG") == "alice.smith@company.org"

    def test_missing_at_raises(self):
        with pytest.raises(ValueError, match="email"):
            format_email("notanemail.com")

    def test_missing_domain_raises(self):
        with pytest.raises(ValueError, match="email"):
            format_email("user@")

    def test_missing_tld_raises(self):
        with pytest.raises(ValueError, match="email"):
            format_email("user@domain")

    def test_whitespace_stripped(self):
        assert format_email("  user@example.com  ") == "user@example.com"

    def test_multiple_at_signs_raises(self):
        with pytest.raises(ValueError, match="email"):
            format_email("a@@b.com")


# ---------------------------------------------------------------------------
# format_date
# ---------------------------------------------------------------------------


class TestFormatDate:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_date(empty) is None

    def test_iso_format_passthrough(self):
        assert format_date("2026-03-19") == "2026-03-19"

    def test_us_slash_format(self):
        assert format_date("3/19/2026") == "2026-03-19"

    def test_long_format(self):
        assert format_date("March 19, 2026") == "2026-03-19"

    def test_two_digit_year_handling(self):
        result = format_date("01/01/99")
        assert result is not None  # dateutil handles this

    def test_custom_output_format(self):
        assert format_date("2026-03-19", format="%m/%d/%Y") == "03/19/2026"

    def test_invalid_date_raises(self):
        with pytest.raises(ValueError, match="date"):
            format_date("not a date at all xyz")

    def test_iso_with_time_parses_date_part(self):
        assert format_date("2026-03-19T14:30:00") == "2026-03-19"

    def test_returns_string(self):
        result = format_date("2026-01-01")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# format_datetime
# ---------------------------------------------------------------------------


class TestFormatDatetime:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_datetime(empty) is None

    def test_iso_format_passthrough(self):
        assert format_datetime("2026-03-19T14:30:00") == "2026-03-19T14:30:00"

    def test_us_slash_with_12h_time(self):
        assert format_datetime("3/19/2026 2:30 PM") == "2026-03-19T14:30:00"

    def test_custom_output_format(self):
        result = format_datetime("2026-03-19 14:30:00", format="%Y/%m/%d %H:%M")
        assert result == "2026/03/19 14:30"

    def test_invalid_datetime_raises(self):
        with pytest.raises(ValueError, match="datetime"):
            format_datetime("banana split")

    def test_midnight(self):
        assert format_datetime("2026-01-01 00:00:00") == "2026-01-01T00:00:00"

    def test_noon(self):
        assert format_datetime("2026-06-15 12:00:00") == "2026-06-15T12:00:00"

    def test_returns_string(self):
        result = format_datetime("2026-01-01T00:00:00")
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# format_enum
# ---------------------------------------------------------------------------


class TestFormatEnum:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_enum(empty) is None

    def test_uppercase_default(self):
        assert format_enum("ca") == "CA"

    def test_lowercase_case(self):
        assert format_enum("CA", case="lower") == "ca"

    def test_preserve_case(self):
        assert format_enum("Active", case="preserve") == "Active"

    def test_valid_value_in_list(self):
        assert format_enum("ca", values=["CA", "NY", "TX"]) == "CA"

    def test_case_insensitive_membership_check(self):
        assert format_enum("ny", values=["CA", "NY", "TX"]) == "NY"

    def test_invalid_value_raises(self):
        with pytest.raises(ValueError, match="enum values"):
            format_enum("ZZ", values=["CA", "NY", "TX"])

    def test_no_values_list_no_error(self):
        assert format_enum("anything") == "ANYTHING"

    def test_unknown_case_raises(self):
        with pytest.raises(ValueError, match="case"):
            format_enum("ca", case="title")

    def test_whitespace_stripped_before_check(self):
        assert format_enum("  ca  ", values=["CA"]) == "CA"


# ---------------------------------------------------------------------------
# format_boolean
# ---------------------------------------------------------------------------


class TestFormatBoolean:
    @pytest.mark.parametrize("empty", [None, "", "   "])
    def test_empty_returns_none(self, empty):
        assert format_boolean(empty) is None

    @pytest.mark.parametrize("truthy", ["true", "True", "TRUE", "yes", "Yes", "1", "on", "ON", "t", "T", "y", "Y"])
    def test_truthy_strings(self, truthy):
        assert format_boolean(truthy) is True

    @pytest.mark.parametrize("falsy", ["false", "False", "FALSE", "no", "No", "0", "off", "OFF", "f", "F", "n", "N"])
    def test_falsy_strings(self, falsy):
        assert format_boolean(falsy) is False

    def test_native_true(self):
        assert format_boolean(True) is True

    def test_native_false(self):
        assert format_boolean(False) is False

    def test_int_one(self):
        assert format_boolean(1) is True

    def test_int_zero(self):
        assert format_boolean(0) is False

    def test_invalid_int_raises(self):
        with pytest.raises(ValueError):
            format_boolean(2)

    def test_invalid_string_raises(self):
        with pytest.raises(ValueError, match="boolean"):
            format_boolean("maybe")

    def test_random_word_raises(self):
        with pytest.raises(ValueError):
            format_boolean("active")


# ---------------------------------------------------------------------------
# format_regex
# ---------------------------------------------------------------------------


class TestFormatRegex:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_regex(empty, pattern=r"\d+") is None

    def test_matching_value_passes_through(self):
        assert format_regex("ABC-1234", pattern=r"[A-Z]+-\d+") == "ABC-1234"

    def test_non_matching_value_raises(self):
        with pytest.raises(ValueError, match="pattern"):
            format_regex("abc1234", pattern=r"[A-Z]+-\d+")

    def test_digits_only_pattern(self):
        assert format_regex("12345", pattern=r"\d+") == "12345"

    def test_partial_match_fails(self):
        # fullmatch is used — partial matches are rejected
        with pytest.raises(ValueError):
            format_regex("123abc", pattern=r"\d+")

    def test_complex_pattern(self):
        assert format_regex("2026-03-19", pattern=r"\d{4}-\d{2}-\d{2}") == "2026-03-19"

    def test_whitespace_stripped_before_match(self):
        assert format_regex("  ABC-1234  ", pattern=r"[A-Z]+-\d+") == "ABC-1234"


# ---------------------------------------------------------------------------
# format_subdivision
# ---------------------------------------------------------------------------


class TestFormatSubdivision:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_subdivision(empty, country_code="US") is None

    def test_lowercase_ca_us(self):
        assert format_subdivision("ca", country_code="US") == "CA"

    def test_uppercase_already(self):
        assert format_subdivision("CA", country_code="US") == "CA"

    def test_ny_us(self):
        assert format_subdivision("NY", country_code="US") == "NY"

    def test_on_canada(self):
        assert format_subdivision("ON", country_code="CA") == "ON"

    def test_invalid_subdivision_raises(self):
        with pytest.raises(ValueError, match="subdivision"):
            format_subdivision("ZZ", country_code="US")

    def test_invalid_country_code_raises(self):
        with pytest.raises(ValueError, match="subdivisions found"):
            format_subdivision("CA", country_code="XX")

    def test_whitespace_stripped(self):
        assert format_subdivision("  tx  ", country_code="US") == "TX"

    def test_returns_local_part_not_full_code(self):
        # Should return "CA", not "US-CA"
        result = format_subdivision("CA", country_code="US")
        assert "-" not in result


# ---------------------------------------------------------------------------
# format_country
# ---------------------------------------------------------------------------


class TestFormatCountry:
    @pytest.mark.parametrize("empty", _EMPTY_VALUES)
    def test_empty_returns_none(self, empty):
        assert format_country(empty) is None

    def test_lowercase_us(self):
        assert format_country("us") == "US"

    def test_uppercase_us(self):
        assert format_country("US") == "US"

    def test_canada(self):
        assert format_country("ca") == "CA"

    def test_gb(self):
        assert format_country("gb") == "GB"

    def test_de_germany(self):
        assert format_country("de") == "DE"

    def test_invalid_code_raises(self):
        with pytest.raises(ValueError, match="alpha-2"):
            format_country("XX")

    def test_three_letter_code_raises(self):
        with pytest.raises(ValueError, match="alpha-2"):
            format_country("USA")

    def test_whitespace_stripped(self):
        assert format_country("  us  ") == "US"

    def test_returns_string(self):
        result = format_country("US")
        assert isinstance(result, str)
