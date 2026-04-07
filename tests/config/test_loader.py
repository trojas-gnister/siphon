"""Tests for siphon.config.loader.load_config."""

from __future__ import annotations

import os

import pytest

from siphon.config.loader import load_config
from siphon.config.schema import SiphonConfig
from siphon.utils.errors import ConfigError

# ---------------------------------------------------------------------------
# Shared YAML fixtures (v2 shape — source: instead of llm:)
# ---------------------------------------------------------------------------

# Minimal but fully valid config — used by most tests as a base.
VALID_YAML = """\
name: test_pipeline
source:
  type: spreadsheet
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

# Valid config with an enum field that has 'values'.
VALID_ENUM_YAML = """\
name: enum_pipeline
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: status
      type: enum
      values:
        - active
        - inactive
      db:
        table: records
        column: status
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
"""

# Valid config with a regex field that has 'pattern'.
VALID_REGEX_YAML = """\
name: regex_pipeline
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: postal_code
      type: regex
      pattern: "^\\\\d{5}$"
      db:
        table: records
        column: postal_code
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
"""

# Valid config with a subdivision field that has 'country_code'.
VALID_SUBDIVISION_YAML = """\
name: subdivision_pipeline
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: state
      type: subdivision
      country_code: US
      db:
        table: records
        column: state
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
"""


# ---------------------------------------------------------------------------
# Happy-path tests
# ---------------------------------------------------------------------------


def test_valid_yaml_loads(tmp_path):
    """A well-formed v2 config file produces a SiphonConfig instance."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(VALID_YAML)

    cfg = load_config(config_file)

    assert isinstance(cfg, SiphonConfig)
    assert cfg.name == "test_pipeline"
    assert cfg.source.type == "spreadsheet"
    assert cfg.database.url == "sqlite:///test.db"


def test_valid_yaml_string_path(tmp_path):
    """load_config accepts a plain string path as well as a Path object."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(VALID_YAML)

    cfg = load_config(str(config_file))

    assert isinstance(cfg, SiphonConfig)


def test_valid_yaml_xml_source(tmp_path):
    """A config with source type xml loads correctly."""
    yaml_text = """\
name: xml_pipeline
source:
  type: xml
  root: Records.Record
  encoding: utf-8
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
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.source.type == "xml"
    assert cfg.source.root == "Records.Record"


def test_env_var_substitution(tmp_path, monkeypatch):
    """${VAR} references in YAML are replaced with the corresponding env var."""
    monkeypatch.setenv("TEST_DB_URL", "sqlite:///subst.db")

    yaml_text = VALID_YAML.replace(
        "url: sqlite:///test.db", "url: ${TEST_DB_URL}"
    )

    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.database.url == "sqlite:///subst.db"


def test_env_var_partial_substitution(tmp_path, monkeypatch):
    """${VAR} can appear as part of a larger string value."""
    monkeypatch.setenv("DB_HOST", "myhost")

    yaml_text = VALID_YAML.replace(
        "url: sqlite:///test.db", "url: postgresql://user:pass@${DB_HOST}/mydb"
    )

    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.database.url == "postgresql://user:pass@myhost/mydb"


def test_dotenv_file_loaded(tmp_path, monkeypatch):
    """Variables defined in a .env file next to the config are substituted."""
    # Ensure the variable is NOT already in the process environment.
    monkeypatch.delenv("SIPHON_DB_URL", raising=False)

    dotenv_file = tmp_path / ".env"
    dotenv_file.write_text("SIPHON_DB_URL=sqlite:///from_dotenv.db\n")

    yaml_text = VALID_YAML.replace(
        "url: sqlite:///test.db", "url: ${SIPHON_DB_URL}"
    )
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.database.url == "sqlite:///from_dotenv.db"


def test_dotenv_does_not_override_existing_env(tmp_path, monkeypatch):
    """An existing process env var takes precedence over a .env file value."""
    monkeypatch.setenv("SIPHON_DB_URL", "sqlite:///from_env.db")

    dotenv_file = tmp_path / ".env"
    dotenv_file.write_text("SIPHON_DB_URL=sqlite:///from_dotenv.db\n")

    yaml_text = VALID_YAML.replace(
        "url: sqlite:///test.db", "url: ${SIPHON_DB_URL}"
    )
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    # python-dotenv's override=False means the process env wins.
    assert cfg.database.url == "sqlite:///from_env.db"


def test_no_dotenv_file_is_fine(tmp_path):
    """load_config works normally when no .env file is present."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(VALID_YAML)

    cfg = load_config(config_file)

    assert isinstance(cfg, SiphonConfig)


def test_enum_field_with_values(tmp_path):
    """An enum field that supplies 'values' passes cross-validation."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(VALID_ENUM_YAML)

    cfg = load_config(config_file)

    assert cfg.schema_.fields[0].type == "enum"


def test_enum_field_with_preset(tmp_path):
    """An enum field that supplies 'preset' passes cross-validation."""
    yaml_text = VALID_ENUM_YAML.replace(
        "      values:\n        - active\n        - inactive\n",
        "      preset: us_states\n",
    )
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.schema_.fields[0].preset == "us_states"


def test_regex_field_with_pattern(tmp_path):
    """A regex field that supplies 'pattern' passes cross-validation."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(VALID_REGEX_YAML)

    cfg = load_config(config_file)

    assert cfg.schema_.fields[0].type == "regex"


def test_subdivision_field_with_country_code(tmp_path):
    """A subdivision field that supplies 'country_code' passes cross-validation."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(VALID_SUBDIVISION_YAML)

    cfg = load_config(config_file)

    assert cfg.schema_.fields[0].country_code == "US"


def test_variables_section_accepted(tmp_path):
    """A config with a 'variables' section loads without errors."""
    yaml_text = """\
name: var_pipeline
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
variables:
  env: production
  batch_size: 50
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
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.variables == {"env": "production", "batch_size": 50}


def test_variables_resolve_in_value_fields(tmp_path, monkeypatch):
    """${var_name} references resolve from the 'variables' section."""
    # Unset so env fallback won't interfere.
    monkeypatch.delenv("MY_DB", raising=False)

    yaml_text = """\
name: var_resolve_pipeline
source:
  type: spreadsheet
database:
  url: ${MY_DB}
variables:
  MY_DB: sqlite:///from_variables.db
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
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.database.url == "sqlite:///from_variables.db"


def test_variables_take_priority_over_env(tmp_path, monkeypatch):
    """Config 'variables' section takes priority over process environment."""
    monkeypatch.setenv("MY_DB", "sqlite:///from_env.db")

    yaml_text = """\
name: var_priority_pipeline
source:
  type: spreadsheet
database:
  url: ${MY_DB}
variables:
  MY_DB: sqlite:///from_variables.db
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
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.database.url == "sqlite:///from_variables.db"


def test_custom_transform_with_transforms_file(tmp_path):
    """A custom transform field with transforms.file configured passes validation."""
    yaml_text = """\
name: custom_transform_pipeline
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
transforms:
  file: transforms/custom.py
schema:
  fields:
    - name: full_name
      type: string
      transform:
        type: custom
        function: build_full_name
        args:
          - first_name
          - last_name
      db:
        table: people
        column: full_name
  tables:
    people:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.schema_.fields[0].transform.type == "custom"
    assert cfg.transforms.file == "transforms/custom.py"


def test_collection_fields_cross_validated_enum_no_values_raises(tmp_path):
    """An enum field inside a collection that lacks both 'values' and 'preset' raises."""
    yaml_text = """\
name: collection_enum_pipeline
source:
  type: xml
  root: Records.Record
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: record_id
      type: string
      db:
        table: records
        column: id
  collections:
    - name: notes
      source_path: Notes.Note
      fields:
        - name: note_status
          type: enum
          db:
            table: notes
            column: status
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
    notes:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="enum"):
        load_config(config_file)


def test_collection_fields_cross_validated_enum_with_values_passes(tmp_path):
    """An enum field inside a collection with 'values' passes cross-validation."""
    yaml_text = """\
name: collection_enum_ok_pipeline
source:
  type: xml
  root: Records.Record
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: record_id
      type: string
      db:
        table: records
        column: id
  collections:
    - name: notes
      source_path: Notes.Note
      fields:
        - name: note_status
          type: enum
          values:
            - open
            - closed
          db:
            table: notes
            column: status
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
    notes:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    cfg = load_config(config_file)

    assert cfg.schema_.collections[0].fields[0].type == "enum"


def test_collection_custom_transform_without_transforms_file_raises(tmp_path):
    """A custom transform on a collection field without transforms.file raises."""
    yaml_text = """\
name: collection_custom_transform_pipeline
source:
  type: xml
  root: Records.Record
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: record_id
      type: string
      db:
        table: records
        column: id
  collections:
    - name: notes
      source_path: Notes.Note
      fields:
        - name: note_text
          type: string
          transform:
            type: custom
            function: clean_note
          db:
            table: notes
            column: text
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
    notes:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="custom"):
        load_config(config_file)


# ---------------------------------------------------------------------------
# Error-path tests
# ---------------------------------------------------------------------------


def test_missing_env_var_raises(tmp_path):
    """Referencing an unset env var raises ConfigError naming the variable."""
    yaml_text = VALID_YAML.replace(
        "url: sqlite:///test.db", "url: ${NONEXISTENT_VAR_XYZ}"
    )
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="NONEXISTENT_VAR_XYZ"):
        load_config(config_file)


def test_invalid_yaml_raises(tmp_path):
    """A file with malformed YAML raises ConfigError."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text("name: [unclosed bracket\n  bad: yaml: here\n")

    with pytest.raises(ConfigError):
        load_config(config_file)


def test_non_mapping_yaml_raises(tmp_path):
    """A YAML file whose root is not a mapping raises ConfigError."""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text("- item1\n- item2\n")

    with pytest.raises(ConfigError):
        load_config(config_file)


def test_pydantic_validation_error_raises_config_error(tmp_path):
    """Missing required fields cause a Pydantic ValidationError wrapped in ConfigError."""
    # 'source' section is entirely absent — Pydantic will reject it.
    yaml_text = """\
name: broken_pipeline
database:
  url: sqlite:///test.db
schema:
  fields: []
  tables: {}
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError):
        load_config(config_file)


def test_enum_without_values_or_preset_raises(tmp_path):
    """An enum field missing both 'values' and 'preset' raises ConfigError."""
    yaml_text = """\
name: bad_enum
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: status
      type: enum
      db:
        table: records
        column: status
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="enum"):
        load_config(config_file)


def test_regex_without_pattern_raises(tmp_path):
    """A regex field missing 'pattern' raises ConfigError."""
    yaml_text = """\
name: bad_regex
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: postal_code
      type: regex
      db:
        table: records
        column: postal_code
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="regex"):
        load_config(config_file)


def test_subdivision_without_country_code_raises(tmp_path):
    """A subdivision field missing 'country_code' raises ConfigError."""
    yaml_text = """\
name: bad_subdivision
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: state
      type: subdivision
      db:
        table: records
        column: state
  tables:
    records:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="subdivision"):
        load_config(config_file)


def test_custom_transform_without_transforms_file_raises(tmp_path):
    """A custom transform field without transforms.file configured raises ConfigError."""
    yaml_text = """\
name: bad_custom_transform
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: full_name
      type: string
      transform:
        type: custom
        function: build_full_name
        args:
          - first_name
          - last_name
      db:
        table: people
        column: full_name
  tables:
    people:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="custom"):
        load_config(config_file)


def test_custom_transform_with_empty_transforms_file_raises(tmp_path):
    """A custom transform field with transforms.file=None raises ConfigError."""
    yaml_text = """\
name: bad_custom_transform_empty
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
transforms:
  file: null
schema:
  fields:
    - name: full_name
      type: string
      transform:
        type: custom
        function: build_full_name
      db:
        table: people
        column: full_name
  tables:
    people:
      primary_key:
        column: id
        type: auto_increment
"""
    config_file = tmp_path / "siphon.yaml"
    config_file.write_text(yaml_text)

    with pytest.raises(ConfigError, match="custom"):
        load_config(config_file)


def test_missing_file_raises_config_error(tmp_path):
    """Passing a path that does not exist raises ConfigError."""
    with pytest.raises(ConfigError):
        load_config(tmp_path / "nonexistent.yaml")
