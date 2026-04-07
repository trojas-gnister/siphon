"""Maps source data to target schema using field configs and transforms."""

from __future__ import annotations

import logging
from typing import Any

from siphon.config.schema import FieldConfig, SiphonConfig, TransformFieldConfig
from siphon.transforms.builtins import BUILTIN_TRANSFORMS

logger = logging.getLogger("siphon")


class Mapper:
    """Applies field mappings and transforms to source records.

    For each field in config.schema_.fields:
    - If field.value is set: use the constant value
    - If field.transform is set (without source): compute via transform
    - If field.source is set: read from source record (with optional alias matching)
    - If field.source + field.transform: read source value, then transform it
    """

    def __init__(
        self,
        config: SiphonConfig,
        custom_transforms: dict[str, callable] | None = None,
    ):
        self._config = config
        self._custom = custom_transforms or {}
        self._variables = config.variables or {}

    def _resolve_source_value(self, record: dict, field: FieldConfig) -> Any:
        """Find a value in a source record using field.source and field.aliases.

        Tries exact match first, then case-insensitive match on source name,
        then case-insensitive match on each alias.
        Returns None if no match found.
        """
        if field.source is None:
            return None

        # Exact match
        if field.source in record:
            return record[field.source]

        # Case-insensitive match on source name
        source_lower = field.source.lower()
        for key in record:
            if key.lower() == source_lower:
                return record[key]

        # Try aliases
        if field.aliases:
            for alias in field.aliases:
                if alias in record:
                    return record[alias]
                alias_lower = alias.lower()
                for key in record:
                    if key.lower() == alias_lower:
                        return record[key]

        return None

    def _apply_transform(
        self,
        transform: TransformFieldConfig,
        value: Any,
        record: dict,
    ) -> Any:
        """Apply a transform to a value.

        Args:
            transform: The transform config
            value: The source field value (may be None for computed fields)
            record: The full source record (for accessing other fields)
        """
        t = transform.type

        if t == "template":
            # Context = variables + all record fields
            context = {**self._variables, **record}
            return BUILTIN_TRANSFORMS["template"](
                value, template=transform.template, context=context
            )

        elif t == "map":
            return BUILTIN_TRANSFORMS["map"](
                value, values=transform.values or {}, default=transform.default
            )

        elif t == "concat":
            # Resolve field names from the record
            field_values = [record.get(f) for f in (transform.fields or [])]
            return BUILTIN_TRANSFORMS["concat"](
                fields=field_values, separator=transform.separator
            )

        elif t == "uuid":
            return BUILTIN_TRANSFORMS["uuid"]()

        elif t == "now":
            kwargs = {}
            if transform.format:
                kwargs["fmt"] = transform.format
            return BUILTIN_TRANSFORMS["now"](**kwargs)

        elif t == "coalesce":
            field_values = [record.get(f) for f in (transform.fields or [])]
            fallback = None
            if transform.fallback:
                fallback = self._apply_transform(transform.fallback, None, record)
            return BUILTIN_TRANSFORMS["coalesce"](
                fields=field_values, fallback=fallback
            )

        elif t == "custom":
            fn = self._custom.get(transform.function)
            if fn is None:
                from siphon.utils.errors import TransformError

                raise TransformError(
                    f"Custom transform '{transform.function}' not found"
                )
            # Resolve args from the record
            args = [record.get(a) for a in (transform.args or [])]
            return fn(*args)

        else:
            from siphon.utils.errors import TransformError

            raise TransformError(f"Unknown transform type: {t}")

    def map_record(self, source_record: dict) -> dict:
        """Map a single source record to target field names.

        Returns a dict keyed by field.name with resolved values.
        """
        result = {}
        for field in self._config.schema_.fields:
            if field.value is not None:
                # Constant value
                result[field.name] = field.value
            elif field.transform and not field.source:
                # Computed field (no source, only transform)
                result[field.name] = self._apply_transform(
                    field.transform, None, source_record
                )
            elif field.source:
                # Source mapping (with optional transform)
                value = self._resolve_source_value(source_record, field)
                if field.transform:
                    value = self._apply_transform(
                        field.transform, value, source_record
                    )
                result[field.name] = value
            else:
                # No source, no value, no standalone transform — default to None
                result[field.name] = None
        return result

    def map_records(self, source_records: list[dict]) -> list[dict]:
        """Map all source records to target field names."""
        return [self.map_record(r) for r in source_records]
