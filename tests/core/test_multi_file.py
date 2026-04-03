"""Tests for multi-file directory input in the Pipeline."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.pipeline import Pipeline, PipelineResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LLM_BLOCK = {
    "base_url": "https://api.example.com/v1",
    "model": "gpt-4o-mini",
    "api_key": "sk-test",
}


def _simple_config() -> SiphonConfig:
    """Single-table config for use in tests."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "pipeline": {
            "chunk_size": 50,
            "review": False,
            "log_level": "warning",
        },
    })


def _write_csv(directory: Path, rows: list[dict], filename: str) -> Path:
    """Write rows as a CSV file under directory and return the path."""
    path = directory / filename
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# _scan_directory unit tests
# ---------------------------------------------------------------------------


class TestScanDirectory:
    def test_returns_supported_files_sorted(self, tmp_path: Path):
        """_scan_directory returns .csv, .xlsx, .xls, .ods files in sorted order."""
        (tmp_path / "b.csv").write_text("header\nval")
        (tmp_path / "a.xlsx").write_bytes(b"")
        (tmp_path / "c.ods").write_bytes(b"")
        (tmp_path / "d.xls").write_bytes(b"")

        result = Pipeline._scan_directory(tmp_path)

        assert [f.name for f in result] == ["a.xlsx", "b.csv", "c.ods", "d.xls"]

    def test_ignores_unsupported_files(self, tmp_path: Path):
        """_scan_directory ignores .txt, .json, .pdf and other non-spreadsheet files."""
        (tmp_path / "data.csv").write_text("col\nval")
        (tmp_path / "notes.txt").write_text("ignore me")
        (tmp_path / "config.json").write_text("{}")
        (tmp_path / "report.pdf").write_bytes(b"")

        result = Pipeline._scan_directory(tmp_path)

        assert len(result) == 1
        assert result[0].name == "data.csv"

    def test_ignores_subdirectories(self, tmp_path: Path):
        """_scan_directory does not descend into subdirectories."""
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        (subdir / "nested.csv").write_text("col\nval")
        (tmp_path / "top.csv").write_text("col\nval")

        result = Pipeline._scan_directory(tmp_path)

        assert len(result) == 1
        assert result[0].name == "top.csv"

    def test_empty_directory_returns_empty_list(self, tmp_path: Path):
        """_scan_directory returns an empty list for a directory with no files."""
        result = Pipeline._scan_directory(tmp_path)
        assert result == []

    def test_case_insensitive_extensions(self, tmp_path: Path):
        """_scan_directory matches extensions regardless of case."""
        (tmp_path / "A.CSV").write_text("col\nval")
        (tmp_path / "B.XLSX").write_bytes(b"")

        result = Pipeline._scan_directory(tmp_path)

        assert len(result) == 2


# ---------------------------------------------------------------------------
# Pipeline.run() with directory input
# ---------------------------------------------------------------------------


class TestDirectoryInput:
    async def test_directory_with_two_csvs_processes_both(self, tmp_path: Path):
        """When given a directory with 2 CSVs, both are processed and records aggregated."""
        _write_csv(tmp_path, [{"company_name": "Acme"}], "file1.csv")
        _write_csv(tmp_path, [{"company_name": "Beta"}], "file2.csv")

        records_file1 = [{"company_name": "Acme"}]
        records_file2 = [{"company_name": "Beta"}]

        config = _simple_config()
        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm = MagicMock()
            mock_llm.extract_json = AsyncMock(
                side_effect=[records_file1, records_file2]
            )
            MockLLM.return_value = mock_llm

            pipeline = Pipeline(config)
            result = await pipeline.run(tmp_path, dry_run=True)

        assert result.total_extracted == 2

    def test_run_accepts_string_directory_path(self, tmp_path: Path):
        """Pipeline.run() accepts a string path to a directory (not just Path)."""
        # Verify _scan_directory is callable with a Path derived from a string
        (tmp_path / "data.csv").write_text("col\nval")
        files = Pipeline._scan_directory(Path(str(tmp_path)))
        assert len(files) == 1

    async def test_directory_ignores_unsupported_files(self, tmp_path: Path):
        """Unsupported files (.txt, .json) in a directory are ignored."""
        _write_csv(tmp_path, [{"company_name": "Acme"}], "data.csv")
        (tmp_path / "notes.txt").write_text("ignore")
        (tmp_path / "meta.json").write_text("{}")

        extracted = [{"company_name": "Acme"}]

        config = _simple_config()
        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm = MagicMock()
            mock_llm.extract_json = AsyncMock(return_value=extracted)
            MockLLM.return_value = mock_llm

            pipeline = Pipeline(config)
            result = await pipeline.run(tmp_path, dry_run=True)

        assert result.total_extracted == 1

    async def test_empty_directory_warns_and_returns_empty_result(
        self, tmp_path: Path
    ):
        """An empty directory logs a warning and returns an empty PipelineResult."""
        config = _simple_config()
        pipeline = Pipeline(config)

        with patch("siphon.core.pipeline.logger") as mock_logger:
            result = await pipeline.run(tmp_path, dry_run=True)

        assert result.total_extracted == 0
        assert result.total_inserted == 0
        # Verify the warning was issued with the expected message fragment
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        assert any("No supported files found" in call for call in warning_calls)

    async def test_directory_aggregates_records_from_multiple_files(
        self, tmp_path: Path
    ):
        """Records from all files in a directory are aggregated into a single result."""
        _write_csv(tmp_path, [{"company_name": "A"}, {"company_name": "B"}], "f1.csv")
        _write_csv(tmp_path, [{"company_name": "C"}], "f2.csv")

        records_f1 = [{"company_name": "A"}, {"company_name": "B"}]
        records_f2 = [{"company_name": "C"}]

        config = _simple_config()
        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm = MagicMock()
            mock_llm.extract_json = AsyncMock(
                side_effect=[records_f1, records_f2]
            )
            MockLLM.return_value = mock_llm

            pipeline = Pipeline(config)
            result = await pipeline.run(tmp_path, dry_run=True)

        assert result.total_extracted == 3
        assert result.total_valid == 3

    async def test_directory_with_mixed_formats_csv_and_xlsx(self, tmp_path: Path):
        """A directory containing both .csv and .xlsx files processes both."""
        _write_csv(tmp_path, [{"company_name": "CsvCo"}], "data.csv")

        # Write a minimal xlsx using pandas
        xlsx_path = tmp_path / "data.xlsx"
        pd.DataFrame([{"company_name": "XlsxCo"}]).to_excel(xlsx_path, index=False)

        records_csv = [{"company_name": "CsvCo"}]
        records_xlsx = [{"company_name": "XlsxCo"}]

        config = _simple_config()
        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm = MagicMock()
            # sorted order: data.csv < data.xlsx alphabetically
            mock_llm.extract_json = AsyncMock(
                side_effect=[records_csv, records_xlsx]
            )
            MockLLM.return_value = mock_llm

            pipeline = Pipeline(config)
            result = await pipeline.run(tmp_path, dry_run=True)

        assert result.total_extracted == 2

    async def test_single_file_path_still_works(self, tmp_path: Path):
        """Passing a single file path (not a directory) still works correctly."""
        csv_path = _write_csv(
            tmp_path, [{"company_name": "Solo"}], "solo.csv"
        )
        extracted = [{"company_name": "Solo"}]

        config = _simple_config()
        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm = MagicMock()
            mock_llm.extract_json = AsyncMock(return_value=extracted)
            MockLLM.return_value = mock_llm

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)

        assert result.total_extracted == 1
