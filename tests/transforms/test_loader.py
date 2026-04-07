"""Tests for custom transform file loader."""

from __future__ import annotations

import pytest

from siphon.transforms.loader import get_custom_transform, load_custom_transforms
from siphon.utils.errors import ConfigError, TransformError


# ---------------------------------------------------------------------------
# Helpers — write temporary transform files
# ---------------------------------------------------------------------------

def _write_valid_transforms(tmp_path):
    """Write a valid transforms file and return its path."""
    p = tmp_path / "my_transforms.py"
    p.write_text(
        "def build_work_location(address, city, state, postal_code, country):\n"
        "    parts = [x for x in [address, city, state, postal_code, country] if x]\n"
        "    return ' '.join(parts)\n"
        "\n"
        "def reverse_name(name):\n"
        "    if not name or ',' not in name:\n"
        "        return name\n"
        "    parts = name.split(',', 1)\n"
        "    return f'{parts[1].strip()} {parts[0].strip()}'\n"
        "\n"
        "def _private_helper():\n"
        "    return 'secret'\n"
        "\n"
        "class MyClass:\n"
        "    pass\n"
        "\n"
        "CONSTANT = 42\n"
    )
    return p


# ---------------------------------------------------------------------------
# load_custom_transforms
# ---------------------------------------------------------------------------

class TestLoadCustomTransforms:
    def test_none_returns_empty_dict(self):
        result = load_custom_transforms(None)
        assert result == {}

    def test_valid_file_returns_dict_of_functions(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        assert "build_work_location" in result
        assert "reverse_name" in result

    def test_private_functions_excluded(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        assert "_private_helper" not in result

    def test_classes_excluded(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        assert "MyClass" not in result

    def test_constants_excluded(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        assert "CONSTANT" not in result

    def test_missing_file_raises_config_error(self, tmp_path):
        missing = tmp_path / "nonexistent.py"
        with pytest.raises(ConfigError, match="Transform file not found"):
            load_custom_transforms(missing)

    def test_invalid_python_raises_config_error(self, tmp_path):
        bad = tmp_path / "bad_syntax.py"
        bad.write_text("def broken(\n    # unclosed paren\n")
        with pytest.raises(ConfigError, match="Failed to load transform file"):
            load_custom_transforms(bad)

    def test_accepts_string_path(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(str(p))
        assert "build_work_location" in result

    def test_all_returned_values_are_callable(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        for name, fn in result.items():
            assert callable(fn), f"Expected callable for '{name}'"

    def test_loaded_function_returns_expected_result(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        fn = result["build_work_location"]
        assert fn("123 Main St", "Springfield", "IL", "62701", "US") == (
            "123 Main St Springfield IL 62701 US"
        )

    def test_reverse_name_returns_expected_result(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        fn = result["reverse_name"]
        assert fn("Smith, John") == "John Smith"

    def test_reverse_name_no_comma_returns_original(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        result = load_custom_transforms(p)
        fn = result["reverse_name"]
        assert fn("John Smith") == "John Smith"


# ---------------------------------------------------------------------------
# get_custom_transform
# ---------------------------------------------------------------------------

class TestGetCustomTransform:
    def test_returns_function_when_found(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        registry = load_custom_transforms(p)
        fn = get_custom_transform(registry, "reverse_name")
        assert callable(fn)

    def test_raises_transform_error_when_not_found(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        registry = load_custom_transforms(p)
        with pytest.raises(TransformError, match="Custom transform 'missing_fn' not found"):
            get_custom_transform(registry, "missing_fn")

    def test_error_message_lists_available_transforms(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        registry = load_custom_transforms(p)
        with pytest.raises(TransformError, match="Available:"):
            get_custom_transform(registry, "nope")

    def test_raises_transform_error_on_empty_registry(self):
        registry = {}
        with pytest.raises(TransformError, match="Custom transform 'any_fn' not found"):
            get_custom_transform(registry, "any_fn")

    def test_returned_function_is_callable_and_works(self, tmp_path):
        p = _write_valid_transforms(tmp_path)
        registry = load_custom_transforms(p)
        fn = get_custom_transform(registry, "build_work_location")
        assert fn("1 Park Ave", "New York", "NY", "10001", "US") == (
            "1 Park Ave New York NY 10001 US"
        )
