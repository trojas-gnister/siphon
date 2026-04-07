"""Tests for error recovery and skip reporting."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.extractor import Extractor
from siphon.core.pipeline import Pipeline
from siphon.utils.errors import ExtractionError


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _make_config(chunk_size: int = 25) -> SiphonConfig:
    """Return a SiphonConfig with the given chunk_size."""
    return SiphonConfig.model_validate(
        {
            "name": "test_error_recovery",
            "source": {"type": "spreadsheet"},
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
                "chunk_size": chunk_size,
                "review": False,
                "log_level": "warning",
            },
        }
    )


def _write_csv(tmp_path: Path, rows: list[dict], filename: str = "data.csv") -> Path:
    """Write a list-of-dicts as a CSV and return the path."""
    df = pd.DataFrame(rows)
    p = tmp_path / filename
    df.to_csv(p, index=False)
    return p


def _make_rows(n: int, prefix: str = "Company") -> list[dict]:
    """Generate n rows with distinct company_name values."""
    return [{"company_name": f"{prefix} {i}"} for i in range(1, n + 1)]


# ---------------------------------------------------------------------------
# 1. Middle chunk failure: chunks 0 and 2 succeed, chunk 1 fails
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_middle_chunk_failure_does_not_abort(tmp_path: Path) -> None:
    """Records from chunks 0 and 2 are returned even when chunk 1 fails."""
    config = _make_config(chunk_size=1)

    rows = _make_rows(3)
    csv_path = _write_csv(tmp_path, rows)

    chunk0_records = [{"company_name": "Company 1"}]
    chunk2_records = [{"company_name": "Company 3"}]

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            chunk0_records,                        # chunk 0: success
            ExtractionError("API timeout"),        # chunk 1: failure
            chunk2_records,                        # chunk 2: success
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert len(records) == 2
    assert {"company_name": "Company 1"} in records
    assert {"company_name": "Company 3"} in records

    assert len(skipped) == 1
    assert skipped[0]["chunk"] == 1


# ---------------------------------------------------------------------------
# 2. Skip report includes row_range with correct values
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_report_includes_row_range(tmp_path: Path) -> None:
    """Skipped chunks report must include a 'row_range' key with correct range."""
    # chunk_size=25 → chunk 0: rows 1-25, chunk 1: rows 26-50, chunk 2: rows 51-75
    config = _make_config(chunk_size=25)

    # 75 rows → 3 chunks of 25
    rows = _make_rows(75)
    csv_path = _write_csv(tmp_path, rows)

    # chunk 0 and 2 succeed, chunk 1 fails
    chunk_records = [{"company_name": f"Company {i}"} for i in range(1, 26)]

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            chunk_records,                    # chunk 0: rows 1-25, success
            ExtractionError("timeout"),       # chunk 1: rows 26-50, failure
            chunk_records,                    # chunk 2: rows 51-75, success
        ]
    )

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)

    assert len(skipped) == 1
    skip = skipped[0]
    assert "row_range" in skip
    assert skip["row_range"] == "rows 26-50"


# ---------------------------------------------------------------------------
# 3. Skip report includes rows_affected count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_report_includes_rows_affected(tmp_path: Path) -> None:
    """Skipped chunks report must include 'rows_affected' with the correct count."""
    config = _make_config(chunk_size=25)

    rows = _make_rows(75)
    csv_path = _write_csv(tmp_path, rows)

    chunk_records = [{"company_name": f"Company {i}"} for i in range(1, 26)]

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            chunk_records,
            ExtractionError("timeout"),
            chunk_records,
        ]
    )

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)

    assert len(skipped) == 1
    assert skipped[0]["rows_affected"] == 25


# ---------------------------------------------------------------------------
# 4. Row count mismatch → retry → skip includes correct row_range
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_row_mismatch_skip_includes_row_range(tmp_path: Path) -> None:
    """When a chunk is skipped due to row-count mismatch, row_range is correct."""
    config = _make_config(chunk_size=25)

    rows = _make_rows(50)
    csv_path = _write_csv(tmp_path, rows)

    chunk0_ok = [{"company_name": f"Company {i}"} for i in range(1, 26)]

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            chunk0_ok,                               # chunk 0: rows 1-25, success
            [{"company_name": "only one"}],          # chunk 1 first: mismatch (25 expected)
            [{"company_name": "still only one"}],   # chunk 1 retry: still mismatch
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert len(records) == 25  # chunk 0 records only
    assert len(skipped) == 1

    skip = skipped[0]
    assert "row_range" in skip
    assert skip["row_range"] == "rows 26-50"
    assert skip["rows_affected"] == 25
    assert "Row count mismatch" in skip["reason"]


# ---------------------------------------------------------------------------
# 5. All chunks fail: empty records, all chunks in skip report
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_chunks_fail_empty_records(tmp_path: Path) -> None:
    """When every chunk fails, records is empty and all chunks appear in skip report."""
    config = _make_config(chunk_size=1)

    rows = _make_rows(3)
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            ExtractionError("fail 0"),
            ExtractionError("fail 1"),
            ExtractionError("fail 2"),
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == []
    assert len(skipped) == 3
    assert {s["chunk"] for s in skipped} == {0, 1, 2}
    for s in skipped:
        assert "row_range" in s
        assert "rows_affected" in s
        assert s["rows_affected"] == 1


# ---------------------------------------------------------------------------
# 6. Skip report structure: all required keys present
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skip_report_structure(tmp_path: Path) -> None:
    """Every skip report entry has chunk, row_range, rows_affected, and reason."""
    config = _make_config(chunk_size=10)

    rows = _make_rows(10)
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=ExtractionError("API error")
    )

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)

    assert len(skipped) == 1
    skip = skipped[0]
    assert "chunk" in skip
    assert "row_range" in skip
    assert "rows_affected" in skip
    assert "reason" in skip
    assert skip["chunk"] == 0
    assert skip["row_range"] == "rows 1-10"
    assert skip["rows_affected"] == 10
    assert "API error" in skip["reason"]


# ---------------------------------------------------------------------------
# 7. First chunk row_range starts at row 1
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_first_chunk_row_range_starts_at_one(tmp_path: Path) -> None:
    """Row numbering is 1-based; the first chunk starts at row 1."""
    config = _make_config(chunk_size=5)

    rows = _make_rows(5)
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(side_effect=ExtractionError("boom"))

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)

    assert skipped[0]["row_range"] == "rows 1-5"


# ---------------------------------------------------------------------------
# 8. Retry-raises skip includes row_range
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_raises_skip_includes_row_range(tmp_path: Path) -> None:
    """When retry raises ExtractionError, the skip report includes row_range."""
    config = _make_config(chunk_size=3)

    rows = _make_rows(3)
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            [{"company_name": "only one"}],       # mismatch (expected 3)
            ExtractionError("retry blew up"),     # retry fails
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == []
    assert len(skipped) == 1
    skip = skipped[0]
    assert skip["row_range"] == "rows 1-3"
    assert skip["rows_affected"] == 3
    assert "Retry failed" in skip["reason"]


# ---------------------------------------------------------------------------
# 9. Validation errors logged with field details (pipeline integration)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validation_errors_logged_with_field_details(
    tmp_path: Path,
) -> None:
    """Invalid records trigger WARNING log messages containing field-level error details."""
    # Build a v2 config with source column mapping
    config = SiphonConfig.model_validate({
        "name": "test_validation_logging",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "source": "company_name",
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
            "review": False,
            "log_level": "warning",
        },
    })

    # Write a CSV where second row has empty company_name (fails required)
    csv_path = _write_csv(tmp_path, [
        {"company_name": "Valid Corp"},
        {"company_name": ""},  # fails required validation
    ])

    log_records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            log_records.append(record)

    capture_handler = _Capture(level=logging.WARNING)

    # Attach our handler directly to the "siphon" logger.
    siphon_logger = logging.getLogger("siphon")
    siphon_logger.setLevel(logging.WARNING)
    siphon_logger.addHandler(capture_handler)

    try:
        with patch("siphon.core.pipeline.setup_logging"):
            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)
    finally:
        siphon_logger.removeHandler(capture_handler)

    assert result.total_invalid == 1

    # There should be at least one warning about validation failure
    validation_warnings = [
        r for r in log_records
        if "Validation failed" in r.getMessage() and r.levelno == logging.WARNING
    ]
    assert len(validation_warnings) >= 1

    # The warning message must contain field-level error detail
    warning_text = validation_warnings[0].getMessage()
    assert "company_name" in warning_text or "required" in warning_text


# ---------------------------------------------------------------------------
# 10. Pipeline result includes skip report with row ranges
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_result_skipped_chunks_empty_on_success(
    tmp_path: Path,
) -> None:
    """PipelineResult.skipped_chunks is empty when all records load successfully."""
    # Build a v2 config with source column mapping
    config = SiphonConfig.model_validate({
        "name": "test_skipped_chunks",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "source": "company_name",
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
            "review": False,
            "log_level": "warning",
        },
    })

    csv_path = _write_csv(tmp_path, [
        {"company_name": "Acme"},
        {"company_name": "Beta"},
        {"company_name": "Gamma"},
    ])

    pipeline = Pipeline(config)
    result = await pipeline.run(csv_path, dry_run=True)

    assert result.total_extracted == 3
    assert result.skipped_chunks == []


# ---------------------------------------------------------------------------
# 11. Multiple skips — correct row ranges for each
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_multiple_skips_have_correct_row_ranges(tmp_path: Path) -> None:
    """When multiple chunks are skipped, each has the correct row_range."""
    config = _make_config(chunk_size=10)

    # 30 rows → chunks: 1-10, 11-20, 21-30
    rows = _make_rows(30)
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            ExtractionError("chunk 0 fail"),
            ExtractionError("chunk 1 fail"),
            ExtractionError("chunk 2 fail"),
        ]
    )

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)

    assert len(skipped) == 3
    # Sort by chunk index to assert deterministically
    skipped_sorted = sorted(skipped, key=lambda s: s["chunk"])
    assert skipped_sorted[0]["row_range"] == "rows 1-10"
    assert skipped_sorted[1]["row_range"] == "rows 11-20"
    assert skipped_sorted[2]["row_range"] == "rows 21-30"
