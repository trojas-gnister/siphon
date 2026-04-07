"""Load custom Python transform files."""

from __future__ import annotations

import importlib.util
import logging
from pathlib import Path

from siphon.utils.errors import ConfigError, TransformError

logger = logging.getLogger("siphon")


def load_custom_transforms(path: str | Path | None) -> dict[str, callable]:
    """Load a Python file and return a dict of {function_name: function}.

    Only includes public callables (names not starting with '_').
    Classes, constants, and imports are excluded.

    Args:
        path: Path to the Python file, or None for an empty registry.

    Returns:
        Dict mapping function names to callables.

    Raises:
        ConfigError: If the file does not exist or cannot be loaded.
    """
    if path is None:
        return {}

    path = Path(path)
    if not path.exists():
        raise ConfigError(f"Transform file not found: {path}")

    try:
        spec = importlib.util.spec_from_file_location("custom_transforms", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    except Exception as e:
        raise ConfigError(f"Failed to load transform file {path}: {e}") from e

    transforms = {}
    for name in dir(module):
        if not name.startswith("_"):
            obj = getattr(module, name)
            if callable(obj) and not isinstance(obj, type):
                transforms[name] = obj

    logger.info("Loaded %d custom transforms from %s", len(transforms), path.name)
    return transforms


def get_custom_transform(
    registry: dict[str, callable], name: str
) -> callable:
    """Get a custom transform function by name.

    Raises TransformError if not found.
    """
    if name not in registry:
        raise TransformError(
            f"Custom transform '{name}' not found. "
            f"Available: {sorted(registry.keys())}"
        )
    return registry[name]
