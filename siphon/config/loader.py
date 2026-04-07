"""YAML config loader with environment variable substitution."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv
from pydantic import ValidationError

from siphon.config.schema import SiphonConfig
from siphon.utils.errors import ConfigError

# Pattern for ${VAR_NAME} references in YAML string values.
_ENV_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _substitute_env_vars(value: Any, variables: dict[str, Any] | None = None) -> Any:
    """Recursively substitute ${VAR_NAME} references in a parsed YAML structure.

    Resolution order:
    1. The ``variables`` dict extracted from the config (if provided).
    2. Process environment variables.

    Raises ConfigError if a referenced variable is not set in either source.
    """
    if isinstance(value, str):
        def _replace(match: re.Match) -> str:
            var_name = match.group(1)
            # 1. Try the config variables section first.
            if variables is not None and var_name in variables:
                return str(variables[var_name])
            # 2. Fall back to process environment.
            result = os.environ.get(var_name)
            if result is None:
                raise ConfigError(
                    f"Environment variable '${{{var_name}}}' is not set"
                )
            return result

        return _ENV_VAR_RE.sub(_replace, value)

    if isinstance(value, dict):
        return {k: _substitute_env_vars(v, variables) for k, v in value.items()}

    if isinstance(value, list):
        return [_substitute_env_vars(item, variables) for item in value]

    return value


def _validate_field(field: Any, config: "SiphonConfig", context: str = "") -> None:
    """Validate a single field's cross-field constraints.

    Args:
        field: The FieldConfig instance to validate.
        config: The top-level SiphonConfig (used to check transforms.file).
        context: Optional human-readable context prefix for error messages
                 (e.g. "collection 'notes' ").
    """
    prefix = f"{context}Field" if not context else f"{context}field"
    if field.type == "enum":
        if field.values is None and field.preset is None:
            raise ConfigError(
                f"{prefix} '{field.name}' has type 'enum' but is missing both "
                "'values' and 'preset'; at least one is required."
            )
    elif field.type == "regex":
        if field.pattern is None:
            raise ConfigError(
                f"{prefix} '{field.name}' has type 'regex' but is missing 'pattern'."
            )
    elif field.type == "subdivision":
        if field.country_code is None:
            raise ConfigError(
                f"{prefix} '{field.name}' has type 'subdivision' but is missing "
                "'country_code'."
            )

    # Custom transforms require transforms.file to be configured.
    if (
        field.transform is not None
        and field.transform.type == "custom"
        and (config.transforms is None or not config.transforms.file)
    ):
        raise ConfigError(
            f"{prefix} '{field.name}' uses transform type 'custom' but "
            "'transforms.file' is not configured."
        )


def _cross_validate(config: SiphonConfig) -> None:
    """Apply cross-validation rules that depend on field `type`.

    Validates both top-level schema fields and fields inside collections.
    Raises ConfigError for any violation.
    """
    for field in config.schema_.fields:
        _validate_field(field, config)

    if config.schema_.collections:
        for collection in config.schema_.collections:
            for field in collection.fields:
                _validate_field(field, config, context=f"In collection '{collection.name}', ")


def load_config(path: str | Path) -> SiphonConfig:
    """Load and validate a Siphon YAML config file.

    Steps:
    1. Look for a .env file in the same directory; load via python-dotenv.
    2. Read the YAML file.
    3. Recursively substitute ${VAR} references — config ``variables`` section
       takes priority, then process environment variables.
    4. Parse into SiphonConfig via Pydantic.
    5. Cross-validate field type requirements.
    6. Return SiphonConfig.

    Raises ConfigError for any issues (bad YAML, missing env vars, validation
    failures, cross-validation failures).
    """
    config_path = Path(path).expanduser().resolve()

    # 1. Load .env from the same directory as the config file (if present).
    dotenv_path = config_path.parent / ".env"
    if dotenv_path.is_file():
        load_dotenv(dotenv_path=dotenv_path, override=False)

    # 2. Read the YAML file.
    try:
        raw_text = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"Cannot read config file '{config_path}': {exc}") from exc

    try:
        raw_data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise ConfigError(f"Invalid YAML in '{config_path}': {exc}") from exc

    if not isinstance(raw_data, dict):
        raise ConfigError(
            f"Config file '{config_path}' must contain a YAML mapping at the top level."
        )

    # 3. Substitute ${VAR} references — config variables take priority over env vars.
    variables: dict[str, Any] | None = raw_data.get("variables")
    try:
        data = _substitute_env_vars(raw_data, variables=variables)
    except ConfigError:
        raise

    # 4. Parse into SiphonConfig.
    try:
        config = SiphonConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(
            f"Config validation failed for '{config_path}': {exc}"
        ) from exc

    # 5. Cross-validate field type requirements.
    _cross_validate(config)

    return config


def validate_config(path: str | Path) -> list[str]:
    """Validate a Siphon YAML config file.

    Loads and validates the config. Returns a list of warnings (non-fatal issues).
    Raises ConfigError for fatal issues (propagated from load_config).

    Warnings may include:
    - No deduplication configured
    - No relationships defined
    - review is disabled
    """
    config = load_config(path)

    warnings: list[str] = []

    if config.schema_.deduplication is None:
        warnings.append(
            "No deduplication configured; duplicate records will not be detected."
        )

    if not config.relationships:
        warnings.append(
            "No relationships defined; all data will be written as flat records."
        )

    if not config.pipeline.review:
        warnings.append(
            "review is disabled; extracted records will not be reviewed before insertion."
        )

    return warnings
