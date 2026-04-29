"""Tests for the CLI's diff rendering."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from siphon.cli import app
from siphon.core.pipeline import PipelineResult

runner = CliRunner()


def _write_valid_config(tmp_path):
    yaml = """
name: test
source: { type: spreadsheet }
database: { url: "sqlite:///t.db" }
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: { table: companies, column: name }
  tables:
    companies:
      primary_key: { column: id, type: auto_increment }
pipeline: { review: false }
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


class TestCLIDiffRendering:
    def test_dry_run_renders_diff_table(self, tmp_path):
        config = _write_valid_config(tmp_path)
        result = PipelineResult(
            total_extracted=3,
            total_valid=3,
            dry_run=True,
            diff={
                "insert": [{"name": "Brand-New"}],
                "update": [{
                    "key": {"name": "Acme"},
                    "changes": {"phone": {"old": "OLD", "new": "NEW"}},
                    "record": {"name": "Acme", "phone": "NEW"},
                }],
                "skip": [],
                "no_change": [{"name": "Same"}],
            },
        )

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv", "--config", str(config), "--dry-run", "--no-review",
            ])

        assert cli_result.exit_code == 0
        assert "Insert" in cli_result.output
        assert "Update" in cli_result.output
        assert "No Change" in cli_result.output
        assert "1" in cli_result.output
        assert "Acme" in cli_result.output

    def test_non_dry_run_does_not_render_diff(self, tmp_path):
        config = _write_valid_config(tmp_path)
        result = PipelineResult(total_extracted=2, total_inserted=2, dry_run=False)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv", "--config", str(config), "--no-review",
            ])

        assert cli_result.exit_code == 0
        assert "No Change" not in cli_result.output


class TestCLIOutputJSON:
    def test_output_json_emits_diff_as_json(self, tmp_path):
        import json
        config = _write_valid_config(tmp_path)
        result = PipelineResult(
            total_extracted=2,
            total_valid=2,
            dry_run=True,
            diff={
                "insert": [{"name": "New"}],
                "update": [],
                "skip": [],
                "no_change": [{"name": "Same"}],
            },
        )

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv",
                "--config", str(config),
                "--dry-run",
                "--no-review",
                "--output", "json",
            ])

        assert cli_result.exit_code == 0
        text = cli_result.output
        first_brace = text.find("{")
        assert first_brace != -1, f"No JSON in output: {text!r}"
        parsed = json.loads(text[first_brace:])
        assert parsed["total_extracted"] == 2
        assert parsed["dry_run"] is True
        assert parsed["diff"]["insert"][0]["name"] == "New"
        assert parsed["diff"]["no_change"][0]["name"] == "Same"

    def test_output_table_is_default(self, tmp_path):
        """Without --output, table format is used."""
        config = _write_valid_config(tmp_path)
        result = PipelineResult(total_extracted=1, dry_run=True, diff={
            "insert": [{"name": "New"}], "update": [], "skip": [], "no_change": []
        })

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=result)
            MockPipeline.return_value = mock_instance

            cli_result = runner.invoke(app, [
                "run", "data.csv",
                "--config", str(config),
                "--dry-run",
                "--no-review",
            ])

        assert cli_result.exit_code == 0
        assert "Pipeline Diff" in cli_result.output
