"""Dynamic Pydantic validator built from a SiphonConfig schema."""

from __future__ import annotations

import logging
from typing import Any

from pydantic import ValidationError, create_model, field_validator

from siphon.config.schema import SiphonConfig
from siphon.config.types import get_formatter, resolve_preset

logger = logging.getLogger(__name__)


class Validator:
    """Validate and format records against a SiphonConfig schema.

    On construction, a dynamic Pydantic model is built from the config's field
    definitions.  Each field gets a ``field_validator`` that calls the
    appropriate formatter with any type-specific keyword arguments collected
    from the FieldConfig.
    """

    def __init__(self, config: SiphonConfig) -> None:
        self._config = config
        self._model = self._build_model()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_model(self):  # noqa: ANN201
        """Build and return a dynamic Pydantic model from the config fields."""
        field_definitions: dict = {}
        validators: dict = {}

        for field_cfg in self._config.schema_.fields:
            # Use Any so Pydantic does not coerce formatter return values
            # (Decimal, int, bool, …) back to str after the field_validator runs.
            # Required fields have no default (Ellipsis); optional fields default to None.
            if field_cfg.required:
                field_definitions[field_cfg.name] = (Any, ...)
            else:
                field_definitions[field_cfg.name] = (Any, None)

            formatter = get_formatter(field_cfg.type)

            # Collect type-specific kwargs for the formatter call
            fmt_kwargs: dict = {}
            ftype = field_cfg.type

            if ftype == "string":
                if field_cfg.min_length is not None:
                    fmt_kwargs["min_length"] = field_cfg.min_length
                if field_cfg.max_length is not None:
                    fmt_kwargs["max_length"] = field_cfg.max_length

            elif ftype in ("integer", "number"):
                if field_cfg.min is not None:
                    fmt_kwargs["min"] = field_cfg.min
                if field_cfg.max is not None:
                    fmt_kwargs["max"] = field_cfg.max

            elif ftype == "enum":
                values = field_cfg.values
                if field_cfg.preset:
                    values = resolve_preset(field_cfg.preset)
                fmt_kwargs["values"] = values
                if field_cfg.case:
                    fmt_kwargs["case"] = field_cfg.case

            elif ftype == "regex":
                fmt_kwargs["pattern"] = field_cfg.pattern

            elif ftype in ("date", "datetime"):
                if field_cfg.format:
                    fmt_kwargs["format"] = field_cfg.format

            elif ftype == "subdivision":
                fmt_kwargs["country_code"] = field_cfg.country_code

            # Build a validator function; use a factory to capture the loop
            # variables by value (closure over fmt, kwargs, is_required, fname).
            validator_fn = _make_validator(
                formatter,
                fmt_kwargs,
                field_cfg.required,
                field_cfg.name,
            )
            validators[f"validate_{field_cfg.name}"] = field_validator(
                field_cfg.name, mode="before"
            )(validator_fn)

        model = create_model("DynamicRecord", **field_definitions, __validators__=validators)
        return model

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_records(
        self, records: list[dict]
    ) -> tuple[list[dict], list[dict]]:
        """Validate and format a list of raw record dicts.

        Parameters
        ----------
        records:
            Raw records, typically produced by the LLM extractor.

        Returns
        -------
        tuple[list[dict], list[dict]]
            A 2-tuple of ``(valid_records, invalid_records)``.

            * ``valid_records`` — records that passed validation, with each
              field value replaced by the formatter's output.
            * ``invalid_records`` — records that failed, each as
              ``{"record": <original>, "errors": <pydantic error list>}``.
        """
        valid: list[dict] = []
        invalid: list[dict] = []

        for record in records:
            try:
                validated = self._model.model_validate(record)
                valid.append(validated.model_dump())
            except ValidationError as exc:
                invalid.append({"record": record, "errors": exc.errors()})

        return valid, invalid

    def deduplicate(
        self,
        records: list[dict],
        existing_keys: set[tuple] | None = None,
    ) -> tuple[list[dict], list[dict]]:
        """Deduplicate records based on config.

        Args:
            records: Validated records to deduplicate
            existing_keys: Pre-loaded keys from DB (if check_db is True).
                          Each key is a tuple of field values, already normalized.

        Returns:
            (unique_records, duplicate_records)
        """
        dedup_config = self._config.schema_.deduplication
        if dedup_config is None:
            return records, []

        key_fields = dedup_config.key
        case_insensitive = dedup_config.match == "case_insensitive"

        seen: set[tuple] = set()
        if existing_keys:
            seen.update(existing_keys)

        unique = []
        duplicates = []

        for record in records:
            key = self._build_dedup_key(record, key_fields, case_insensitive)
            if key in seen:
                duplicates.append(record)
                logger.warning(f"Duplicate record skipped: {dict(zip(key_fields, key))}")
            else:
                seen.add(key)
                unique.append(record)

        return unique, duplicates

    def _build_dedup_key(
        self,
        record: dict,
        key_fields: list[str],
        case_insensitive: bool,
    ) -> tuple:
        """Build a dedup key tuple from a record."""
        values = []
        for field in key_fields:
            value = record.get(field, "")
            if value is None:
                value = ""
            if case_insensitive and isinstance(value, str):
                value = value.lower()
            values.append(value)
        return tuple(values)

    @staticmethod
    def build_existing_keys(
        rows: list[dict],
        key_fields: list[str],
        case_insensitive: bool,
    ) -> set[tuple]:
        """Build a set of dedup keys from existing DB rows.

        This is a static method so it can be called by the pipeline
        to pre-load keys from the database before deduplication.
        """
        keys = set()
        for row in rows:
            values = []
            for field in key_fields:
                value = row.get(field, "")
                if value is None:
                    value = ""
                if case_insensitive and isinstance(value, str):
                    value = value.lower()
                values.append(value)
            keys.add(tuple(values))
        return keys


# ---------------------------------------------------------------------------
# Factory helper (module-level so the closure captures values, not references)
# ---------------------------------------------------------------------------


def _make_validator(fmt, kwargs: dict, is_required: bool, fname: str):
    """Return a ``field_validator``-compatible classmethod function."""

    def _validate(cls, v):  # noqa: ANN001, ANN202
        # Normalise NaN floats that may arrive from pandas DataFrames
        if isinstance(v, float) and str(v) == "nan":
            v = None

        # Strip strings and treat empty strings as None
        if isinstance(v, str):
            v = v.strip()
            if v == "":
                v = None

        if v is None:
            if is_required:
                raise ValueError(f"{fname} is required")
            return None

        return fmt(v, **kwargs)

    return _validate
