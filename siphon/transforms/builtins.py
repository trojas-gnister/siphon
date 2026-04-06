"""Built-in transform functions for the Siphon ETL pipeline."""

from __future__ import annotations

import uuid as _uuid
from datetime import datetime, timezone
from typing import Any


def transform_template(value: Any, *, template: str, context: dict) -> str:
    """Apply a template string using context variables.

    {value} is replaced with the source field value.
    Other {keys} are replaced from the context dict.
    Unknown keys are left as-is.

    Examples:
        transform_template("abc-123", template="{prefix}-{value}", context={"prefix": "MFRM"})
        → "MFRM-abc-123"
    """
    return template.format(value=value if value is not None else "", **context)


def transform_map(value: Any, *, values: dict, default: Any = None) -> Any:
    """Map a value through a lookup dict.

    Examples:
        transform_map("Closed", values={"Closed": 8, "Open": 3}, default=0)
        → 8
        transform_map("Unknown", values={"Closed": 8}, default=0)
        → 0
    """
    if value is None:
        return default
    return values.get(str(value), default)


def transform_concat(*, fields: list[Any], separator: str = " ") -> str:
    """Concatenate non-empty field values with a separator.

    Skips None and empty-string values.

    Examples:
        transform_concat(fields=["123 Main", "Springfield", "IL"], separator=", ")
        → "123 Main, Springfield, IL"
        transform_concat(fields=["hello", None, "", "world"])
        → "hello world"
    """
    parts = [str(v) for v in fields if v is not None and str(v).strip()]
    return separator.join(parts) if parts else ""


def transform_uuid() -> str:
    """Generate a UUID4 string.

    Returns a lowercase hex UUID: "550e8400-e29b-41d4-a716-446655440000"
    """
    return str(_uuid.uuid4())


def transform_now(*, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Return current UTC timestamp as a formatted string.

    Args:
        fmt: strftime format string. Default: "%Y-%m-%d %H:%M:%S"
    """
    return datetime.now(timezone.utc).strftime(fmt)


def transform_coalesce(*, fields: list[Any], fallback: Any = None) -> Any:
    """Return the first non-null, non-empty value from fields.

    Examples:
        transform_coalesce(fields=[None, "", "hello"])
        → "hello"
        transform_coalesce(fields=[None, None], fallback="default")
        → "default"
    """
    for v in fields:
        if v is not None and str(v).strip():
            return v
    return fallback


BUILTIN_TRANSFORMS: dict[str, callable] = {
    "template": transform_template,
    "map": transform_map,
    "concat": transform_concat,
    "uuid": transform_uuid,
    "now": transform_now,
    "coalesce": transform_coalesce,
}
