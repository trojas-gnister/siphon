"""Tests for siphon.config.loader.validate_config."""

from __future__ import annotations

import pytest

from siphon.config import validate_config
from siphon.utils.errors import ConfigError

# ---------------------------------------------------------------------------
# Shared YAML helpers
# ---------------------------------------------------------------------------

# Minimal valid config — no deduplication, no relationships, review=False.
# validate_config should return warnings but NOT raise.
MINIMAL_VALID_YAML = """\
name: test_pipeline
llm:
  base_url: http://localhost:11434/v1
  model: llama3
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: company_name
      type: string
      db:
        table: companies
        column: name
  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment
pipeline:
  chunk_size: 10
  log_level: info
"""

# Fully-featured valid config — deduplication, relationships, review=True.
# validate_config should return an empty warnings list.
FULL_VALID_YAML = """\
name: full_pipeline
llm:
  base_url: http://localhost:11434/v1
  model: llama3
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: company_name
      type: string
      db:
        table: companies
        column: name
    - name: industry_id
      type: integer
      db:
        table: companies
        column: industry_id
  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment
    industries:
      primary_key:
        column: id
        type: auto_increment
  deduplication:
    key:
      - company_name
    check_db: false
    match: exact
relationships:
  - type: belongs_to
    field: industry_id
    table: companies
    references: industries
    fk_column: industry_id
    resolve_by: name
pipeline:
  chunk_size: 25
  review: true
  log_level: info
"""

# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_valid_config_returns_no_errors(tmp_path):
    """validate_config on a fully-configured pipeline raises no exception."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(FULL_VALID_YAML)

    warnings = validate_config(config_file)

    assert isinstance(warnings, list)
    assert warnings == [], f"Expected no warnings, got: {warnings}"


def test_valid_config_accepts_string_path(tmp_path):
    """validate_config accepts a plain string path as well as a Path object."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(FULL_VALID_YAML)

    warnings = validate_config(str(config_file))

    assert isinstance(warnings, list)


def test_minimal_config_returns_warnings(tmp_path):
    """A valid but minimal config (no dedup, no relationships, review off) returns warnings."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(MINIMAL_VALID_YAML)

    warnings = validate_config(config_file)

    assert isinstance(warnings, list)
    assert len(warnings) >= 1, "Expected at least one warning for missing deduplication"


def test_no_deduplication_warning(tmp_path):
    """A config without deduplication produces a deduplication warning."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(MINIMAL_VALID_YAML)

    warnings = validate_config(config_file)

    assert any("deduplication" in w.lower() for w in warnings)


def test_no_relationships_warning(tmp_path):
    """A config without relationships produces a relationships warning."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(MINIMAL_VALID_YAML)

    warnings = validate_config(config_file)

    assert any("relationship" in w.lower() for w in warnings)


def test_review_disabled_warning(tmp_path):
    """A config with review=False produces a review warning."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(MINIMAL_VALID_YAML)

    warnings = validate_config(config_file)

    assert any("review" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# Error-path tests
# ---------------------------------------------------------------------------


def test_invalid_config_raises_config_error(tmp_path):
    """validate_config raises ConfigError when required fields are missing."""
    invalid_yaml = """\
name: broken_pipeline
database:
  url: sqlite:///test.db
schema:
  fields: []
  tables: {}
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(invalid_yaml)

    with pytest.raises(ConfigError):
        validate_config(config_file)


def test_missing_file_raises_config_error(tmp_path):
    """validate_config raises ConfigError when the file does not exist."""
    with pytest.raises(ConfigError):
        validate_config(tmp_path / "nonexistent.yaml")


def test_bad_yaml_raises_config_error(tmp_path):
    """validate_config raises ConfigError for malformed YAML."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text("name: [unclosed bracket\n  bad: yaml: here\n")

    with pytest.raises(ConfigError):
        validate_config(config_file)
