"""Prompt builders for the Siphon LLM extraction pipeline."""

from __future__ import annotations

from siphon.config.schema import FieldConfig
from siphon.config.types import resolve_preset

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_MAX_ENUM_DISPLAY = 10
_ENUM_HEAD = 5
_ENUM_TAIL = 2


def _format_enum_values(values: list[str]) -> str:
    """Format enum values for display in a prompt.

    If there are more than _MAX_ENUM_DISPLAY values, show the first
    _ENUM_HEAD and the last _ENUM_TAIL separated by "...".
    """
    if len(values) <= _MAX_ENUM_DISPLAY:
        return ", ".join(values)
    head = values[:_ENUM_HEAD]
    tail = values[-_ENUM_TAIL:]
    return ", ".join(head) + ", ..., " + ", ".join(tail)


def _field_description(field: FieldConfig) -> str:
    """Generate a human-readable description for a field to include in a prompt.

    Format:
      - string:      "name (string, required)"  (required appended when True)
      - enum:        "name (enum: A, B, C)"
      - subdivision: "name (subdivision, country: US)"
      - country:     "name (ISO 3166-1 country code)"
      - phone:       "name (phone number)"
      - url:         "name (url)"
      - other:       "name (type)"
    """
    name = field.name
    ftype = field.type

    if ftype == "country":
        type_desc = "ISO 3166-1 country code"
    elif ftype == "phone":
        type_desc = "phone number"
    elif ftype == "subdivision":
        country = field.country_code or "unknown"
        type_desc = f"subdivision, country: {country}"
    elif ftype == "enum":
        # Resolve values: explicit list takes priority, then preset
        if field.values:
            values = field.values
        elif field.preset:
            values = resolve_preset(field.preset)
        else:
            values = []

        if values:
            type_desc = f"enum: {_format_enum_values(values)}"
        else:
            type_desc = "enum"
    else:
        type_desc = ftype

    suffix = ", required" if field.required else ""
    return f"{name} ({type_desc}{suffix})"


# ---------------------------------------------------------------------------
# Public prompt builders
# ---------------------------------------------------------------------------


def build_extraction_prompt(
    fields: list[FieldConfig],
    chunk_csv: str,
    row_count: int,
    extraction_hints: str | None = None,
) -> str:
    """Build the extraction prompt from schema fields and CSV data.

    Parameters
    ----------
    fields:
        The list of FieldConfig objects describing what to extract.
    chunk_csv:
        The raw CSV text for this chunk.
    row_count:
        The number of data rows in chunk_csv (excluding the header).
    extraction_hints:
        Optional free-text instructions from the LLM config to append.

    Returns
    -------
    str
        A fully-formed prompt string ready to send to the LLM.
    """
    field_lines = "\n".join(f"- {_field_description(f)}" for f in fields)

    hints_section = ""
    if extraction_hints:
        hints_section = f"\nAdditional instructions:\n{extraction_hints}\n"

    return (
        "You are a data extraction assistant. Given CSV data, extract the\n"
        "following fields for EACH row:\n"
        "\n"
        "Fields to extract:\n"
        f"{field_lines}\n"
        f"{hints_section}"
        "\n"
        "Rules:\n"
        f"- Return a JSON array with exactly {row_count} objects, one per input row\n"
        "- Every object must have all fields listed above\n"
        '- Use empty string "" for missing values\n'
        "- Do not skip or duplicate rows\n"
        "\n"
        "CSV data:\n"
        f"{chunk_csv}"
    )


def build_revision_prompt(
    batch_json: str,
    command: str,
) -> str:
    """Build the revision prompt for HITL review.

    Parameters
    ----------
    batch_json:
        The current batch of extracted data serialised as a JSON string.
    command:
        The user's natural-language modification instruction.

    Returns
    -------
    str
        A fully-formed revision prompt ready to send to the LLM.
    """
    return (
        "Here is a batch of extracted data as JSON:\n"
        f"{batch_json}\n"
        "\n"
        "Apply this modification:\n"
        f'"{command}"\n'
        "\n"
        "Return the modified data as a JSON array with the same structure."
    )


def build_correction_prompt(
    expected: int,
    actual: int,
) -> str:
    """Build the correction prompt for row count mismatches.

    Parameters
    ----------
    expected:
        The number of rows the LLM should have returned.
    actual:
        The number of rows the LLM actually returned.

    Returns
    -------
    str
        A short correction prompt asking the LLM to return the right count.
    """
    return (
        f"You returned {actual} objects but the input had {expected} rows. "
        f"Return exactly {expected} objects."
    )
