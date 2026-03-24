"""Pure formatting functions for all 14 Siphon field types.

Each formatter accepts a raw value and optional keyword arguments.
Empty/None/whitespace-only values return None.
Invalid values raise ValueError.
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any

import pycountry
from dateutil import parser as dateutil_parser


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMPTY = (None, "")


def _is_empty(value: Any) -> bool:
    """Return True if value is None, empty string, or whitespace-only string."""
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


# ---------------------------------------------------------------------------
# string
# ---------------------------------------------------------------------------


def format_string(
    value: Any,
    *,
    min_length: int | None = None,
    max_length: int | None = None,
) -> str | None:
    """Strip whitespace. Enforce optional min/max length constraints."""
    if _is_empty(value):
        return None

    result = str(value).strip()

    if result == "":
        return None

    if min_length is not None and len(result) < min_length:
        raise ValueError(
            f"String length {len(result)} is less than min_length={min_length}"
        )
    if max_length is not None and len(result) > max_length:
        raise ValueError(
            f"String length {len(result)} exceeds max_length={max_length}"
        )

    return result


# ---------------------------------------------------------------------------
# integer
# ---------------------------------------------------------------------------


def format_integer(
    value: Any,
    *,
    min: int | None = None,
    max: int | None = None,
) -> int | None:
    """Cast to int. Enforce optional min/max constraints."""
    if _is_empty(value):
        return None

    try:
        # Handle float strings like "42.0" gracefully
        result = int(float(str(value).strip()))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot convert {value!r} to integer") from exc

    if min is not None and result < min:
        raise ValueError(f"Integer {result} is less than min={min}")
    if max is not None and result > max:
        raise ValueError(f"Integer {result} exceeds max={max}")

    return result


# ---------------------------------------------------------------------------
# number
# ---------------------------------------------------------------------------


def format_number(
    value: Any,
    *,
    min: float | None = None,
    max: float | None = None,
) -> float | None:
    """Cast to float. Enforce optional min/max constraints."""
    if _is_empty(value):
        return None

    try:
        result = float(str(value).strip())
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Cannot convert {value!r} to number") from exc

    if min is not None and result < min:
        raise ValueError(f"Number {result} is less than min={min}")
    if max is not None and result > max:
        raise ValueError(f"Number {result} exceeds max={max}")

    return result


# ---------------------------------------------------------------------------
# currency
# ---------------------------------------------------------------------------


def format_currency(value: Any) -> Decimal | None:
    """Strip currency symbols/commas, handle parenthetical negatives.

    Returns a Decimal rounded to 2 decimal places.
    """
    if _is_empty(value):
        return None

    raw = str(value).strip()

    # Detect parenthetical negatives like (1,234.56)
    negative = False
    if raw.startswith("(") and raw.endswith(")"):
        negative = True
        raw = raw[1:-1]

    # Strip currency symbols, whitespace, commas
    raw = re.sub(r"[\$,\s]", "", raw)

    # Handle explicit leading minus (e.g. "-$1,234")
    if raw.startswith("-"):
        negative = True
        raw = raw[1:]

    if not raw:
        return None

    try:
        result = Decimal(raw)
    except InvalidOperation as exc:
        raise ValueError(f"Cannot convert {value!r} to currency Decimal") from exc

    if negative:
        result = -result

    return result.quantize(Decimal("0.01"))


# ---------------------------------------------------------------------------
# phone
# ---------------------------------------------------------------------------


def format_phone(value: Any) -> str | None:
    """Strip non-digits; accept 10 or 11 digits (11 must start with 1).

    Returns (XXX) XXX-XXXX.
    """
    if _is_empty(value):
        return None

    digits = re.sub(r"\D", "", str(value))

    if len(digits) == 11:
        if digits[0] != "1":
            raise ValueError(
                f"11-digit phone number must start with country code 1, got {digits[0]!r}"
            )
        digits = digits[1:]

    if len(digits) != 10:
        raise ValueError(
            f"Phone number must have 10 or 11 digits, got {len(digits)} digits"
        )

    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"


# ---------------------------------------------------------------------------
# url
# ---------------------------------------------------------------------------


def format_url(value: Any) -> str | None:
    """Prepend http:// if no scheme present. Validate basic URL structure."""
    if _is_empty(value):
        return None

    raw = str(value).strip()

    # Prepend scheme if missing
    if not re.match(r"^[a-zA-Z][a-zA-Z\d+\-.]*://", raw):
        raw = "http://" + raw

    # Basic validation: must have a host component after the scheme
    if not re.match(r"^[a-zA-Z][a-zA-Z\d+\-.]*://[^\s/$.?#][^\s]*$", raw):
        raise ValueError(f"Invalid URL: {value!r}")

    return raw


# ---------------------------------------------------------------------------
# email
# ---------------------------------------------------------------------------


def format_email(value: Any) -> str | None:
    """Lowercase and validate basic email structure (has @ with domain)."""
    if _is_empty(value):
        return None

    result = str(value).strip().lower()

    # RFC-basic: local@domain.tld
    if not re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", result):
        raise ValueError(f"Invalid email address: {value!r}")

    return result


# ---------------------------------------------------------------------------
# date
# ---------------------------------------------------------------------------


def format_date(
    value: Any,
    *,
    format: str = "%Y-%m-%d",
) -> str | None:
    """Parse any reasonable date string and return it formatted."""
    if _is_empty(value):
        return None

    raw = str(value).strip()
    try:
        parsed = dateutil_parser.parse(raw)
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"Cannot parse date from {value!r}") from exc

    return parsed.strftime(format)


# ---------------------------------------------------------------------------
# datetime
# ---------------------------------------------------------------------------


def format_datetime(
    value: Any,
    *,
    format: str = "%Y-%m-%dT%H:%M:%S",
) -> str | None:
    """Parse any reasonable datetime string and return it formatted."""
    if _is_empty(value):
        return None

    raw = str(value).strip()
    try:
        parsed = dateutil_parser.parse(raw)
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"Cannot parse datetime from {value!r}") from exc

    return parsed.strftime(format)


# ---------------------------------------------------------------------------
# enum
# ---------------------------------------------------------------------------


def format_enum(
    value: Any,
    *,
    values: list[str] | None = None,
    case: str = "upper",
) -> str | None:
    """Validate enum membership (case-insensitive) and apply case transform."""
    if _is_empty(value):
        return None

    raw = str(value).strip()

    if values is not None:
        lower_values = [v.lower() for v in values]
        if raw.lower() not in lower_values:
            raise ValueError(
                f"Value {raw!r} is not in allowed enum values: {values}"
            )

    if case == "upper":
        return raw.upper()
    elif case == "lower":
        return raw.lower()
    elif case == "preserve":
        return raw
    else:
        raise ValueError(f"Unknown case transform: {case!r}. Use 'upper', 'lower', or 'preserve'")


# ---------------------------------------------------------------------------
# boolean
# ---------------------------------------------------------------------------

_TRUTHY = {"true", "yes", "1", "on", "t", "y"}
_FALSY = {"false", "no", "0", "off", "f", "n"}


def format_boolean(value: Any) -> bool | None:
    """Map common truthy/falsy string representations to Python bool."""
    if value is None:
        return None
    if isinstance(value, str) and value.strip() == "":
        return None

    # Native bool / int shortcut
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if value == 1:
            return True
        if value == 0:
            return False
        raise ValueError(f"Integer {value!r} is not a valid boolean (use 0 or 1)")

    lowered = str(value).strip().lower()
    if lowered in _TRUTHY:
        return True
    if lowered in _FALSY:
        return False

    raise ValueError(f"Cannot convert {value!r} to boolean")


# ---------------------------------------------------------------------------
# regex
# ---------------------------------------------------------------------------


def format_regex(value: Any, *, pattern: str) -> str | None:
    """Validate that value matches the given regex pattern. Returns as-is."""
    if _is_empty(value):
        return None

    raw = str(value).strip()

    if not re.fullmatch(pattern, raw):
        raise ValueError(
            f"Value {raw!r} does not match pattern {pattern!r}"
        )

    return raw


# ---------------------------------------------------------------------------
# subdivision
# ---------------------------------------------------------------------------


def format_subdivision(value: Any, *, country_code: str) -> str | None:
    """Validate ISO 3166-2 subdivision code and return the local part uppercased.

    For example, with country_code="US", "ca" -> "CA".
    The local part is the portion after the hyphen in the full code (e.g. "US-CA").
    """
    if _is_empty(value):
        return None

    local = str(value).strip().upper()
    cc = country_code.strip().upper()

    # Collect all subdivision local codes for this country
    subdivisions = pycountry.subdivisions.get(country_code=cc)
    if not subdivisions:
        raise ValueError(f"No subdivisions found for country code {cc!r}")

    # Each subdivision code is like "US-CA"; extract the local part after "-"
    local_codes = {sub.code.split("-", 1)[1] for sub in subdivisions}

    if local not in local_codes:
        raise ValueError(
            f"Subdivision {local!r} is not a valid subdivision code for country {cc!r}"
        )

    return local


# ---------------------------------------------------------------------------
# country
# ---------------------------------------------------------------------------


def format_country(value: Any) -> str | None:
    """Validate ISO 3166-1 alpha-2 country code and return it uppercased."""
    if _is_empty(value):
        return None

    code = str(value).strip().upper()

    country = pycountry.countries.get(alpha_2=code)
    if country is None:
        raise ValueError(f"{code!r} is not a valid ISO 3166-1 alpha-2 country code")

    return country.alpha_2
