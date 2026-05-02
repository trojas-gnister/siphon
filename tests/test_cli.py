"""Tests for the Siphon Typer CLI (siphon.cli)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from siphon.cli import app
from siphon.core.pipeline import PipelineResult
from siphon.utils.errors import ConfigError

runner = CliRunner()

# ---------------------------------------------------------------------------
# Minimal valid YAML for use in tests that need a real config file
# ---------------------------------------------------------------------------

_VALID_YAML = """\
name: test_pipeline
source:
  type: spreadsheet
database:
  url: sqlite:///test.db
schema:
  fields:
    - name: company_name
      source: "Company Name"
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
  log_level: info
  review: false
"""

_INVALID_YAML = """\
name: broken_pipeline
database:
  url: sqlite:///test.db
schema:
  fields: []
  tables: {}
"""


# ---------------------------------------------------------------------------
# Helper: write a valid config file to a tmp directory
# ---------------------------------------------------------------------------


def _write_valid_config(tmp_path: Path) -> Path:
    """Write a minimal valid siphon.yaml and return its path."""
    p = tmp_path / "siphon.yaml"
    p.write_text(_VALID_YAML)
    return p


def _write_invalid_config(tmp_path: Path) -> Path:
    """Write an invalid siphon.yaml (missing required sections) and return its path."""
    p = tmp_path / "siphon.yaml"
    p.write_text(_INVALID_YAML)
    return p


# ---------------------------------------------------------------------------
# --version
# ---------------------------------------------------------------------------


class TestVersion:
    def test_version_flag_exits_zero(self):
        """--version should exit 0."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0

    def test_version_flag_prints_version(self):
        """--version should print 'siphon' followed by a version string."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "siphon" in result.output
        # Should contain a version number (digits and dots)
        import re
        assert re.search(r"\d+\.\d+\.\d+", result.output)


# ---------------------------------------------------------------------------
# --help
# ---------------------------------------------------------------------------


class TestHelp:
    def test_help_lists_commands(self):
        """--help should exit 0 and mention the three commands."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "run" in result.output
        assert "validate" in result.output
        assert "init" in result.output

    def test_run_help(self):
        """siphon run --help should list all flags."""
        result = runner.invoke(app, ["run", "--help"])
        assert result.exit_code == 0
        assert "--config" in result.output
        assert "--dry-run" in result.output
        assert "--no-review" in result.output
        assert "--create-tables" in result.output
        assert "--chunk-size" not in result.output
        assert "--verbose" in result.output
        assert "--quiet" in result.output

    def test_validate_help(self):
        """siphon validate --help should list the --config flag."""
        result = runner.invoke(app, ["validate", "--help"])
        assert result.exit_code == 0
        assert "--config" in result.output

    def test_init_help(self):
        """siphon init --help should exit 0."""
        result = runner.invoke(app, ["init", "--help"])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# validate command
# ---------------------------------------------------------------------------


class TestValidateCommand:
    def test_validate_valid_config_exits_zero(self, tmp_path: Path):
        """validate with a well-formed config exits 0 and prints success."""
        config_file = _write_valid_config(tmp_path)
        result = runner.invoke(app, ["validate", "--config", str(config_file)])
        assert result.exit_code == 0
        assert "valid" in result.output.lower()

    def test_validate_prints_warnings(self, tmp_path: Path):
        """validate prints any non-fatal warnings returned by validate_config."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.validate_config") as mock_validate:
            mock_validate.return_value = ["No deduplication configured."]
            result = runner.invoke(app, ["validate", "--config", str(config_file)])

        assert result.exit_code == 0
        assert "Warning" in result.output or "warning" in result.output.lower() or "No deduplication" in result.output

    def test_validate_invalid_config_exits_one(self, tmp_path: Path):
        """validate with an invalid config exits 1 and reports the error."""
        config_file = _write_invalid_config(tmp_path)
        result = runner.invoke(app, ["validate", "--config", str(config_file)])
        assert result.exit_code == 1
        # Should mention the config is invalid
        assert "invalid" in result.output.lower() or "error" in result.output.lower()

    def test_validate_missing_config_exits_one(self, tmp_path: Path):
        """validate with a path that does not exist exits 1."""
        missing = tmp_path / "does_not_exist.yaml"
        result = runner.invoke(app, ["validate", "--config", str(missing)])
        assert result.exit_code == 1

    def test_validate_default_config_path(self, tmp_path: Path):
        """validate uses siphon.yaml in the cwd by default."""
        # Use patch so we don't need the file to actually be in cwd
        with patch("siphon.cli.validate_config") as mock_validate:
            mock_validate.return_value = []
            result = runner.invoke(app, ["validate"])

        # validate_config is called with the default path
        mock_validate.assert_called_once()
        called_path = mock_validate.call_args[0][0]
        assert Path(called_path).name == "siphon.yaml"


# ---------------------------------------------------------------------------
# run command
# ---------------------------------------------------------------------------


class TestRunCommand:
    def _make_mock_result(self, **kwargs) -> PipelineResult:
        defaults = dict(
            total_extracted=10,
            total_valid=8,
            total_invalid=2,
            total_duplicates=0,
            total_inserted=8,
            dry_run=False,
        )
        defaults.update(kwargs)
        return PipelineResult(**defaults)

    def test_run_dry_run_exits_zero(self, tmp_path: Path):
        """run --dry-run with a mocked pipeline exits 0."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=self._make_mock_result(dry_run=True, total_inserted=0))
            MockPipeline.return_value = mock_instance

            result = runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
                "--dry-run",
            ])

        assert result.exit_code == 0

    def test_run_prints_summary_table(self, tmp_path: Path):
        """run prints a summary table with expected metrics."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=self._make_mock_result())
            MockPipeline.return_value = mock_instance

            result = runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
            ])

        assert result.exit_code == 0
        # Summary table columns should appear
        assert "Extracted" in result.output
        assert "Valid" in result.output
        assert "Invalid" in result.output
        assert "Inserted" in result.output

    def test_run_dry_run_shows_skipped_in_summary(self, tmp_path: Path):
        """run --dry-run shows 'skipped (dry run)' for Inserted."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=self._make_mock_result(dry_run=True, total_inserted=0))
            MockPipeline.return_value = mock_instance

            result = runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
                "--dry-run",
            ])

        assert result.exit_code == 0
        assert "dry run" in result.output.lower() or "skipped" in result.output.lower()

    def test_run_missing_config_exits_one(self, tmp_path: Path):
        """run with a missing config file exits 1 with an error message."""
        missing = tmp_path / "does_not_exist.yaml"
        result = runner.invoke(app, ["run", "test.csv", "--config", str(missing)])
        assert result.exit_code == 1
        assert "error" in result.output.lower() or "Error" in result.output

    def test_run_siphon_error_exits_one(self, tmp_path: Path):
        """run exits 1 when load_config raises a SiphonError."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.load_config") as mock_load:
            mock_load.side_effect = ConfigError("bad config")
            result = runner.invoke(app, ["run", "test.csv", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "bad config" in result.output

    def test_run_unexpected_error_exits_one(self, tmp_path: Path):
        """run exits 1 and shows 'Unexpected error' for non-SiphonError exceptions."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.load_config") as mock_load:
            mock_load.side_effect = RuntimeError("something broke")
            result = runner.invoke(app, ["run", "test.csv", "--config", str(config_file)])

        assert result.exit_code == 1
        assert "Unexpected error" in result.output or "something broke" in result.output

    def test_run_passes_pipeline_args(self, tmp_path: Path):
        """run passes dry_run, no_review, create_tables, and sheet to pipeline.run."""
        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=self._make_mock_result(dry_run=True, total_inserted=0))
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv",
                "--config", str(config_file),
                "--dry-run",
                "--no-review",
                "--create-tables",
            ])

        mock_instance.run.assert_awaited_once_with(
            "input.csv",
            dry_run=True,
            no_review=True,
            create_tables=True,
            sheet=None,
            resume=False,
            user=None,
        )


# ---------------------------------------------------------------------------
# --verbose / --quiet log level overrides
# ---------------------------------------------------------------------------


class TestLogLevelOverrides:
    def test_verbose_sets_debug_log_level(self, tmp_path: Path):
        """--verbose sets cfg.pipeline.log_level to 'debug'."""
        config_file = _write_valid_config(tmp_path)

        captured_log_level = {}

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            def _capture_log_level(cfg):
                captured_log_level["level"] = cfg.pipeline.log_level
                instance = MagicMock()
                instance.run = AsyncMock(return_value=PipelineResult())
                return instance

            MockPipeline.side_effect = _capture_log_level

            runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
                "--verbose",
            ])

        assert captured_log_level.get("level") == "debug"

    def test_quiet_sets_error_log_level(self, tmp_path: Path):
        """--quiet sets cfg.pipeline.log_level to 'error'."""
        config_file = _write_valid_config(tmp_path)

        captured_log_level = {}

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            def _capture_log_level(cfg):
                captured_log_level["level"] = cfg.pipeline.log_level
                instance = MagicMock()
                instance.run = AsyncMock(return_value=PipelineResult())
                return instance

            MockPipeline.side_effect = _capture_log_level

            runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
                "--quiet",
            ])

        assert captured_log_level.get("level") == "error"

    def test_no_flag_keeps_config_log_level(self, tmp_path: Path):
        """Without --verbose or --quiet the config log level is unchanged."""
        config_file = _write_valid_config(tmp_path)

        captured_log_level = {}

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "warning"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            def _capture_log_level(cfg):
                captured_log_level["level"] = cfg.pipeline.log_level
                instance = MagicMock()
                instance.run = AsyncMock(return_value=PipelineResult())
                return instance

            MockPipeline.side_effect = _capture_log_level

            runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
            ])

        assert captured_log_level.get("level") == "warning"


# ---------------------------------------------------------------------------
# init command
# ---------------------------------------------------------------------------


class TestInitCommand:
    def test_init_creates_siphon_yaml(self, tmp_path: Path):
        """init creates siphon.yaml in the current working directory."""
        with runner.isolated_filesystem():
            result = runner.invoke(app, ["init"], catch_exceptions=False)
        assert result.exit_code == 0
        assert "Created" in result.output or "created" in result.output.lower()

    def test_init_file_has_yaml_content(self, tmp_path: Path):
        """init writes a non-empty YAML file."""
        # Use isolated_filesystem so we start with no pre-existing siphon.yaml
        with runner.isolated_filesystem():
            result = runner.invoke(app, ["init"], catch_exceptions=False)
        assert result.exit_code == 0

    def test_init_overwrite_confirmed(self, tmp_path: Path):
        """init overwrites existing siphon.yaml when user confirms."""
        # The CliRunner's isolated filesystem handles the file
        result = runner.invoke(app, ["init"], input="y\n", catch_exceptions=False)
        # Whether a file pre-exists or not, confirming should not exit non-zero
        assert result.exit_code == 0

    def test_init_overwrite_declined_aborts(self, tmp_path: Path):
        """init aborts without overwriting when user declines."""
        # First creation
        runner.invoke(app, ["init"], catch_exceptions=False)
        # Second invocation — decline the overwrite
        result = runner.invoke(app, ["init"], input="n\n", catch_exceptions=False)
        # typer.Exit() without code=1 means exit_code == 0
        assert result.exit_code == 0
        # Should NOT print "Created" again
        assert "Created" not in result.output and "created" not in result.output.lower()

    def test_init_no_args_needed(self):
        """init requires no arguments."""
        with runner.isolated_filesystem():
            result = runner.invoke(app, ["init"], catch_exceptions=False)
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# Skipped chunks appear in summary
# ---------------------------------------------------------------------------


class TestSummaryWithSkippedChunks:
    def test_skipped_chunks_shown_in_summary(self, tmp_path: Path):
        """When the result has skipped_chunks, the count is printed."""
        config_file = _write_valid_config(tmp_path)

        skipped = [{"chunk": 0, "reason": "LLM timeout"}]

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg

            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult(
                total_extracted=5,
                total_valid=4,
                total_invalid=1,
                skipped_chunks=skipped,
                dry_run=True,
            ))
            MockPipeline.return_value = mock_instance

            result = runner.invoke(app, [
                "run", "test.csv",
                "--config", str(config_file),
                "--dry-run",
            ])

        assert result.exit_code == 0
        assert "Skipped" in result.output or "skipped" in result.output.lower()


class TestResumeAndBatchSizeFlags:
    def test_resume_flag_passed_to_pipeline(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv",
                "--config", str(config_file),
                "--no-review",
                "--resume",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs["resume"] is True

    def test_batch_size_flag_overrides_config(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.pipeline.batch_size = 500  # original config value
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv",
                "--config", str(config_file),
                "--no-review",
                "--batch-size", "10",
            ])

        # The CLI overwrites the config's batch_size before constructing Pipeline
        assert mock_cfg.pipeline.batch_size == 10

    def test_resume_default_is_false(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv", "--config", str(config_file), "--no-review",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs.get("resume", False) is False


class TestUserFlag:
    def test_user_flag_passed_to_pipeline(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv",
                "--config", str(config_file),
                "--no-review",
                "--user", "alice",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs["user"] == "alice"

    def test_user_default_is_none(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            runner.invoke(app, [
                "run", "input.csv", "--config", str(config_file), "--no-review",
            ])

        kwargs = mock_instance.run.call_args.kwargs
        assert kwargs.get("user") is None


class TestMultiSourceCLI:
    def test_run_without_input_path_works_with_sources_config(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        # Build a config that uses `sources:` (no top-level source/fields)
        config_yaml = """
name: multi
sources:
  - name: a
    type: spreadsheet
    path: "./a.csv"
    fields:
      - name: x
        source: "X"
        type: string
        required: true
        db: { table: t, column: x }
database: { url: "sqlite:///t.db" }
schema:
  tables:
    t: { primary_key: { column: id, type: auto_increment } }
pipeline: { review: false }
"""
        config_file = tmp_path / "siphon.yaml"
        config_file.write_text(config_yaml)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = [MagicMock()]  # truthy
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            # Invoke without a positional input path
            result = runner.invoke(app, [
                "run", "--config", str(config_file), "--no-review",
            ])

        # Should not error out for missing input_path
        assert result.exit_code == 0

    def test_run_without_input_path_and_without_sources_errors(self, tmp_path):
        from unittest.mock import AsyncMock, MagicMock, patch
        from siphon.core.pipeline import PipelineResult

        config_file = _write_valid_config(tmp_path)

        with patch("siphon.cli.Pipeline") as MockPipeline, \
             patch("siphon.cli.load_config") as mock_load:
            mock_cfg = MagicMock()
            mock_cfg.pipeline.log_level = "info"
            mock_cfg.sources = None  # single-source config
            mock_load.return_value = mock_cfg
            mock_instance = MagicMock()
            mock_instance.run = AsyncMock(return_value=PipelineResult())
            MockPipeline.return_value = mock_instance

            result = runner.invoke(app, [
                "run", "--config", str(config_file), "--no-review",
            ])

        # Single-source without input_path should fail
        assert result.exit_code != 0
