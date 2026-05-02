"""Tests for the `siphon init` template (Task 21)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from siphon.cli import INIT_TEMPLATE, app

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _invoke_init_in_tmpdir() -> tuple[object, Path]:
    """Run `siphon init` in an isolated filesystem and return (result, yaml_path)."""
    with runner.isolated_filesystem() as td:
        result = runner.invoke(app, ["init"], catch_exceptions=False)
        yaml_path = Path(td) / "siphon.yaml"
        content = yaml_path.read_text() if yaml_path.exists() else ""
    return result, yaml_path, content


# ---------------------------------------------------------------------------
# 1. init creates siphon.yaml in the working directory
# ---------------------------------------------------------------------------


class TestInitCreatesFile:
    def test_creates_siphon_yaml(self):
        """init writes siphon.yaml to the current working directory."""
        with runner.isolated_filesystem() as td:
            result = runner.invoke(app, ["init"], catch_exceptions=False)
            assert result.exit_code == 0
            assert (Path(td) / "siphon.yaml").exists()

    def test_output_mentions_created(self):
        """init prints a confirmation message."""
        with runner.isolated_filesystem():
            result = runner.invoke(app, ["init"], catch_exceptions=False)
        assert result.exit_code == 0
        assert "created" in result.output.lower() or "Created" in result.output


# ---------------------------------------------------------------------------
# 2. Created file is valid YAML
# ---------------------------------------------------------------------------


class TestInitFileIsValidYaml:
    def test_file_is_parseable_yaml(self):
        """The generated siphon.yaml must parse without errors."""
        with runner.isolated_filesystem() as td:
            runner.invoke(app, ["init"], catch_exceptions=False)
            content = (Path(td) / "siphon.yaml").read_text()
        # Should not raise
        parsed = yaml.safe_load(content)
        assert parsed is not None

    def test_template_constant_is_valid_yaml(self):
        """INIT_TEMPLATE itself must be valid YAML."""
        parsed = yaml.safe_load(INIT_TEMPLATE)
        assert parsed is not None

    def test_parsed_yaml_has_expected_top_level_keys(self):
        """The parsed YAML should contain name, source, database, schema, pipeline."""
        parsed = yaml.safe_load(INIT_TEMPLATE)
        for key in ("name", "source", "database", "schema", "pipeline"):
            assert key in parsed, f"Missing top-level key: {key}"


# ---------------------------------------------------------------------------
# 3. Template includes comments for all 14 field types
# ---------------------------------------------------------------------------


FIELD_TYPES = [
    "string",
    "integer",
    "number",
    "currency",
    "phone",
    "url",
    "email",
    "date",
    "datetime",
    "enum",
    "boolean",
    "regex",
    "subdivision",
    "country",
]


class TestAllFieldTypesPresent:
    @pytest.mark.parametrize("field_type", FIELD_TYPES)
    def test_field_type_mentioned(self, field_type: str):
        """INIT_TEMPLATE must reference each of the 14 field types."""
        assert field_type in INIT_TEMPLATE, (
            f"Field type '{field_type}' not found in INIT_TEMPLATE"
        )

    def test_all_14_field_types_present(self):
        """All 14 field types must appear somewhere in the template."""
        missing = [ft for ft in FIELD_TYPES if ft not in INIT_TEMPLATE]
        assert not missing, f"Missing field types: {missing}"


# ---------------------------------------------------------------------------
# 4. Template includes source config section
# ---------------------------------------------------------------------------


class TestSourceSection:
    def test_source_section_present_in_template(self):
        """INIT_TEMPLATE must contain a source: section."""
        assert "source:" in INIT_TEMPLATE

    def test_source_has_type(self):
        """source section should include a type field."""
        assert "type:" in INIT_TEMPLATE

    def test_parsed_source_keys(self):
        """Parsed YAML source section must contain a type key."""
        parsed = yaml.safe_load(INIT_TEMPLATE)
        source = parsed.get("source", {})
        assert "type" in source, "source.type missing from parsed template"


# ---------------------------------------------------------------------------
# 5. Template includes database config section
# ---------------------------------------------------------------------------


class TestDatabaseSection:
    def test_database_section_present(self):
        """INIT_TEMPLATE must contain a database: section."""
        assert "database:" in INIT_TEMPLATE

    def test_database_has_url(self):
        """database section should include a url field."""
        assert "url" in INIT_TEMPLATE

    def test_parsed_database_url(self):
        """Parsed YAML database section must contain a url key."""
        parsed = yaml.safe_load(INIT_TEMPLATE)
        assert "url" in parsed.get("database", {})


# ---------------------------------------------------------------------------
# 6. Template includes relationships examples (commented)
# ---------------------------------------------------------------------------


class TestRelationshipsSection:
    def test_relationships_mentioned(self):
        """INIT_TEMPLATE must mention 'relationships'."""
        assert "relationships" in INIT_TEMPLATE

    def test_belongs_to_example(self):
        """Template should include a belongs_to relationship example."""
        assert "belongs_to" in INIT_TEMPLATE

    def test_junction_example(self):
        """Template should include a junction relationship example."""
        assert "junction" in INIT_TEMPLATE

    def test_subdivision_example_has_country_code(self):
        """Subdivision field example should include country_code."""
        assert "country_code" in INIT_TEMPLATE


# ---------------------------------------------------------------------------
# 7. Template includes pipeline section
# ---------------------------------------------------------------------------


class TestPipelineSection:
    def test_pipeline_section_present(self):
        """INIT_TEMPLATE must contain a pipeline: section."""
        assert "pipeline:" in INIT_TEMPLATE

    def test_pipeline_has_review(self):
        """pipeline section should include review flag."""
        assert "review" in INIT_TEMPLATE

    def test_pipeline_has_log_level(self):
        """pipeline section should include log_level."""
        assert "log_level" in INIT_TEMPLATE

    def test_parsed_pipeline_keys(self):
        """Parsed YAML pipeline section must contain review and log_level."""
        parsed = yaml.safe_load(INIT_TEMPLATE)
        pipeline = parsed.get("pipeline", {})
        for key in ("review", "log_level"):
            assert key in pipeline, f"pipeline.{key} missing from parsed template"


def test_init_template_includes_on_conflict_example(tmp_path, monkeypatch):
    """The init template should include a commented on_conflict example."""
    from typer.testing import CliRunner
    from siphon.cli import app
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()
    runner.invoke(app, ["init"], input="y\n")

    content = (tmp_path / "siphon.yaml").read_text()
    assert "on_conflict" in content
    assert "action:" in content
    assert "update" in content
    assert "skip" in content
    assert "error" in content
