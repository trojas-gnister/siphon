"""Field type registry mapping the 14 Siphon type names to formatters and SQL types."""

from __future__ import annotations

import pycountry
from sqlalchemy import types as sa_types

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
# Registry
# ---------------------------------------------------------------------------

FIELD_TYPE_REGISTRY: dict[str, dict] = {
    "string": {
        "formatter": format_string,
        "sql_type": sa_types.String(255),
        "options": ["min_length", "max_length"],
    },
    "integer": {
        "formatter": format_integer,
        "sql_type": sa_types.Integer(),
        "options": ["min", "max"],
    },
    "number": {
        "formatter": format_number,
        "sql_type": sa_types.Float(),
        "options": ["min", "max"],
    },
    "currency": {
        "formatter": format_currency,
        "sql_type": sa_types.Numeric(12, 2),
        "options": [],
    },
    "phone": {
        "formatter": format_phone,
        "sql_type": sa_types.String(20),
        "options": [],
    },
    "url": {
        "formatter": format_url,
        "sql_type": sa_types.String(500),
        "options": [],
    },
    "email": {
        "formatter": format_email,
        "sql_type": sa_types.String(255),
        "options": [],
    },
    "date": {
        "formatter": format_date,
        "sql_type": sa_types.Date(),
        "options": ["format"],
    },
    "datetime": {
        "formatter": format_datetime,
        "sql_type": sa_types.DateTime(),
        "options": ["format"],
    },
    "enum": {
        "formatter": format_enum,
        "sql_type": sa_types.String(50),
        "options": ["values", "preset", "case"],
    },
    "boolean": {
        "formatter": format_boolean,
        "sql_type": sa_types.Boolean(),
        "options": [],
    },
    "regex": {
        "formatter": format_regex,
        "sql_type": sa_types.String(255),
        "options": ["pattern"],
    },
    "subdivision": {
        "formatter": format_subdivision,
        "sql_type": sa_types.String(10),
        "options": ["country_code"],
    },
    "country": {
        "formatter": format_country,
        "sql_type": sa_types.String(2),
        "options": [],
    },
}

# ---------------------------------------------------------------------------
# Public accessors
# ---------------------------------------------------------------------------

_PRESET_MAP: dict[str, str] = {
    "us_states": "US",
    "ca_provinces": "CA",
}


def get_formatter(type_name: str):
    """Return the formatter function for a given type name.

    Raises ValueError if the type name is not registered.
    """
    entry = FIELD_TYPE_REGISTRY.get(type_name)
    if entry is None:
        raise ValueError(
            f"Unknown field type: {type_name!r}. "
            f"Known types: {sorted(FIELD_TYPE_REGISTRY)}"
        )
    return entry["formatter"]


def get_sql_type(type_name: str):
    """Return the SQLAlchemy type instance for a given type name.

    Raises ValueError if the type name is not registered.
    """
    entry = FIELD_TYPE_REGISTRY.get(type_name)
    if entry is None:
        raise ValueError(
            f"Unknown field type: {type_name!r}. "
            f"Known types: {sorted(FIELD_TYPE_REGISTRY)}"
        )
    return entry["sql_type"]


def resolve_preset(preset_name: str) -> list[str]:
    """Resolve a preset name to a sorted list of subdivision codes.

    Supported presets:
    - ``"us_states"``    → US state/territory codes via pycountry
    - ``"ca_provinces"`` → Canadian province/territory codes via pycountry

    Raises ValueError for unknown preset names.
    """
    country_code = _PRESET_MAP.get(preset_name)
    if country_code is None:
        raise ValueError(
            f"Unknown preset: {preset_name!r}. "
            f"Known presets: {sorted(_PRESET_MAP)}"
        )

    subdivisions = pycountry.subdivisions.get(country_code=country_code)
    return sorted([s.code.split("-")[1] for s in subdivisions])
