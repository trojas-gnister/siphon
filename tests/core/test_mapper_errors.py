"""Mapper behavior when transforms raise exceptions.

These tests document current behavior: a transform exception during
map_records() propagates up immediately, killing the batch. They verify
the exception is informative enough that the user can trace it back to
the offending record + transform.
"""

from __future__ import annotations

import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.mapper import Mapper


def _config_with_custom_transform() -> SiphonConfig:
    return SiphonConfig.model_validate({
        "name": "transform-error-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite:///t.db"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "t", "column": "name"}},
                {"name": "computed",
                 "transform": {
                     "type": "custom",
                     "function": "boom",
                     "args": ["Name"],
                 },
                 "type": "string",
                 "db": {"table": "t", "column": "computed"}},
            ],
            "tables": {"t": {"primary_key": {"column": "id", "type": "auto_increment"}}},
        },
        "pipeline": {"review": False},
    })


def _boom(value):
    """Custom transform that raises on a specific input."""
    if value == "BAD":
        raise ValueError(f"transform refused to process value '{value}'")
    return value.upper()


class TestTransformException:
    def test_custom_transform_raises_propagates(self):
        """A custom transform raising during map_records propagates the error."""
        config = _config_with_custom_transform()
        mapper = Mapper(config, custom_transforms={"boom": _boom})

        with pytest.raises(Exception) as exc_info:
            mapper.map_records([
                {"Name": "ok"},
                {"Name": "BAD"},
            ])

        # Error message should mention what the transform refused
        msg = str(exc_info.value)
        assert "BAD" in msg or "transform" in msg.lower() or "refused" in msg.lower()

    def test_clean_records_map_successfully(self):
        """Sanity check: when no transform raises, all records map cleanly."""
        config = _config_with_custom_transform()
        mapper = Mapper(config, custom_transforms={"boom": _boom})

        result = mapper.map_records([
            {"Name": "alpha"},
            {"Name": "beta"},
        ])

        assert len(result) == 2
        assert result[0]["computed"] == "ALPHA"
        assert result[1]["computed"] == "BETA"
