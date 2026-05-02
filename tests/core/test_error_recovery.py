"""Tests for error recovery and pipeline result reporting."""

from __future__ import annotations

import logging
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.pipeline import Pipeline


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
# 1. Validation errors logged with field details (pipeline integration)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_validation_errors_logged_with_field_details(
    tmp_path: Path,
) -> None:
    """Invalid records trigger WARNING log messages containing field-level error details."""
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
# 2. Pipeline result skipped_chunks is empty on success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pipeline_result_skipped_chunks_empty_on_success(
    tmp_path: Path,
) -> None:
    """PipelineResult.skipped_chunks is empty when all records load successfully."""
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
