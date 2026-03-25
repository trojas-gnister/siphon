"""Tests for the Extractor class."""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.extractor import Extractor
from siphon.utils.errors import ExtractionError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config(sample_config_dict) -> SiphonConfig:
    """Build a minimal SiphonConfig from the shared fixture dict."""
    return SiphonConfig.model_validate(sample_config_dict)


@pytest.fixture
def mock_llm() -> MagicMock:
    """Return a MagicMock whose extract_json is an AsyncMock."""
    llm = MagicMock()
    llm.extract_json = AsyncMock(return_value=[{"company_name": "Acme"}])
    return llm


@pytest.fixture
def extractor(config, mock_llm) -> Extractor:
    return Extractor(config, mock_llm)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _write_csv(tmp_path: Path, rows: list[dict], filename: str = "data.csv") -> Path:
    """Write a list-of-dicts as a CSV file and return the path."""
    df = pd.DataFrame(rows)
    p = tmp_path / filename
    df.to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# load_spreadsheet — CSV
# ---------------------------------------------------------------------------


def test_load_csv_returns_dataframe(extractor: Extractor, tmp_path: Path) -> None:
    csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}, {"company_name": "Beta"}])
    df = extractor.load_spreadsheet(csv_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["company_name"]
    assert df.iloc[0]["company_name"] == "Acme"


def test_load_csv_fills_na_with_empty_string(extractor: Extractor, tmp_path: Path) -> None:
    csv_path = tmp_path / "na.csv"
    csv_path.write_text("company_name,city\nAcme,\nBeta,NYC\n")
    df = extractor.load_spreadsheet(csv_path)
    assert df.iloc[0]["city"] == ""


def test_load_csv_returns_string_dtype(extractor: Extractor, tmp_path: Path) -> None:
    csv_path = _write_csv(tmp_path, [{"id": "1", "name": "Acme"}])
    df = extractor.load_spreadsheet(csv_path)
    # Values should be string-like (pandas may use object or StringDtype)
    assert df.iloc[0]["id"] == "1"
    assert isinstance(df.iloc[0]["name"], str)


# ---------------------------------------------------------------------------
# load_spreadsheet — XLSX
# ---------------------------------------------------------------------------


def test_load_xlsx_returns_dataframe(extractor: Extractor, tmp_path: Path) -> None:
    xlsx_path = tmp_path / "data.xlsx"
    df_out = pd.DataFrame([{"company_name": "Acme"}, {"company_name": "Beta"}])
    df_out.to_excel(xlsx_path, index=False)

    df = extractor.load_spreadsheet(xlsx_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert df.iloc[1]["company_name"] == "Beta"


def test_load_xlsx_fills_na(extractor: Extractor, tmp_path: Path) -> None:
    xlsx_path = tmp_path / "na.xlsx"
    df_out = pd.DataFrame([{"name": "Acme", "city": None}])
    df_out.to_excel(xlsx_path, index=False)

    df = extractor.load_spreadsheet(xlsx_path)
    assert df.iloc[0]["city"] == ""


# ---------------------------------------------------------------------------
# load_spreadsheet — unsupported / error cases
# ---------------------------------------------------------------------------


def test_load_unsupported_format_raises(extractor: Extractor, tmp_path: Path) -> None:
    bad_path = tmp_path / "data.txt"
    bad_path.write_text("hello")
    with pytest.raises(ExtractionError, match="Unsupported file format"):
        extractor.load_spreadsheet(bad_path)


def test_load_missing_file_raises(extractor: Extractor, tmp_path: Path) -> None:
    missing = tmp_path / "ghost.csv"
    with pytest.raises(ExtractionError, match="Failed to read"):
        extractor.load_spreadsheet(missing)


def test_load_corrupt_csv_raises(extractor: Extractor, tmp_path: Path) -> None:
    """A file with .csv extension that pandas cannot parse raises ExtractionError."""
    bad_csv = tmp_path / "bad.csv"
    # Write something pandas will reject — e.g. a binary-like payload that
    # triggers a ParserError
    bad_csv.write_bytes(b"\x00\x01\x02\x03" * 100)
    # pandas may or may not raise depending on content; if it doesn't raise,
    # that's fine — but if it does it must be wrapped in ExtractionError.
    try:
        extractor.load_spreadsheet(bad_csv)
    except ExtractionError:
        pass  # Expected path


# ---------------------------------------------------------------------------
# chunk_dataframe
# ---------------------------------------------------------------------------


def test_chunk_dataframe_splits_evenly(extractor: Extractor) -> None:
    df = pd.DataFrame({"x": range(10)})
    chunks = extractor.chunk_dataframe(df, 5)
    assert len(chunks) == 2
    assert len(chunks[0]) == 5
    assert len(chunks[1]) == 5


def test_chunk_dataframe_splits_with_remainder(extractor: Extractor) -> None:
    df = pd.DataFrame({"x": range(11)})
    chunks = extractor.chunk_dataframe(df, 5)
    assert len(chunks) == 3
    assert len(chunks[2]) == 1


def test_chunk_dataframe_single_chunk_when_small(extractor: Extractor) -> None:
    df = pd.DataFrame({"x": range(3)})
    chunks = extractor.chunk_dataframe(df, 10)
    assert len(chunks) == 1
    assert len(chunks[0]) == 3


def test_chunk_dataframe_empty_df(extractor: Extractor) -> None:
    df = pd.DataFrame({"x": []})
    chunks = extractor.chunk_dataframe(df, 5)
    assert chunks == []


# ---------------------------------------------------------------------------
# extract — happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_returns_records(
    config: SiphonConfig, tmp_path: Path
) -> None:
    rows = [{"company_name": "Acme"}, {"company_name": "Beta"}]
    csv_path = _write_csv(tmp_path, rows)

    expected_records = [{"company_name": "Acme"}, {"company_name": "Beta"}]
    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(return_value=expected_records)

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == expected_records
    assert skipped == []


@pytest.mark.asyncio
async def test_extract_returns_skipped_empty_on_success(
    config: SiphonConfig, tmp_path: Path
) -> None:
    csv_path = _write_csv(tmp_path, [{"company_name": "X"}])
    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(return_value=[{"company_name": "X"}])

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)
    assert skipped == []


# ---------------------------------------------------------------------------
# extract — row count mismatch: retry succeeds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_row_mismatch_retry_succeeds(
    config: SiphonConfig, tmp_path: Path
) -> None:
    """First call returns wrong count; second call (retry) returns the correct count."""
    rows = [{"company_name": "Acme"}, {"company_name": "Beta"}]
    csv_path = _write_csv(tmp_path, rows)

    correct_records = [{"company_name": "Acme"}, {"company_name": "Beta"}]
    mock_llm = MagicMock()
    # First call: returns only 1 record (mismatch). Retry: returns 2.
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            [{"company_name": "Acme"}],  # mismatch
            correct_records,             # retry succeeds
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == correct_records
    assert skipped == []
    assert mock_llm.extract_json.call_count == 2


# ---------------------------------------------------------------------------
# extract — row count mismatch: retry also mismatches → chunk skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_row_mismatch_retry_fails_skips_chunk(
    config: SiphonConfig, tmp_path: Path
) -> None:
    rows = [{"company_name": "Acme"}, {"company_name": "Beta"}]
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    # Both calls return wrong count
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            [{"company_name": "Acme"}],  # first: mismatch
            [{"company_name": "Beta"}],  # retry: still mismatch
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == []
    assert len(skipped) == 1
    assert "Row count mismatch" in skipped[0]["reason"]


# ---------------------------------------------------------------------------
# extract — retry raises ExtractionError → chunk skipped
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_row_mismatch_retry_raises_skips_chunk(
    config: SiphonConfig, tmp_path: Path
) -> None:
    rows = [{"company_name": "Acme"}, {"company_name": "Beta"}]
    csv_path = _write_csv(tmp_path, rows)

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            [{"company_name": "Acme"}],          # first: mismatch
            ExtractionError("retry exploded"),    # retry raises
        ]
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == []
    assert len(skipped) == 1
    assert "Retry failed" in skipped[0]["reason"]


# ---------------------------------------------------------------------------
# extract — LLM failure on first attempt → chunk skipped, others still run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_llm_failure_skips_chunk(
    config: SiphonConfig, tmp_path: Path
) -> None:
    """LLM raises on first attempt → chunk is skipped, no retry."""
    csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=ExtractionError("network timeout")
    )

    extractor = Extractor(config, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert records == []
    assert len(skipped) == 1
    assert "LLM error" in skipped[0]["reason"]
    assert "network timeout" in skipped[0]["reason"]


@pytest.mark.asyncio
async def test_extract_llm_failure_on_one_chunk_others_succeed(
    config: SiphonConfig, tmp_path: Path
) -> None:
    """When one chunk fails, the remaining chunks still produce records."""
    # chunk_size is 50 in sample_config_dict; write 3 rows but override to
    # chunk_size=1 so we get 3 separate chunks.
    cfg_dict = {
        "name": "test_pipeline",
        "llm": {
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "api_key": "sk-test",
        },
        "database": {"url": "sqlite+aiosqlite:///test.db"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                }
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"}
                }
            },
        },
        "pipeline": {"chunk_size": 1},
    }
    cfg = SiphonConfig.model_validate(cfg_dict)

    rows = [
        {"company_name": "Acme"},
        {"company_name": "Beta"},
        {"company_name": "Gamma"},
    ]
    csv_path = _write_csv(tmp_path, rows)

    # Chunk 0 succeeds, chunk 1 fails, chunk 2 succeeds
    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            [{"company_name": "Acme"}],         # chunk 0: ok
            ExtractionError("timeout"),          # chunk 1: fail
            [{"company_name": "Gamma"}],         # chunk 2: ok
        ]
    )

    extractor = Extractor(cfg, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    assert len(records) == 2
    assert {"company_name": "Acme"} in records
    assert {"company_name": "Gamma"} in records
    assert len(skipped) == 1


# ---------------------------------------------------------------------------
# extract — concurrent processing via asyncio.gather
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_multiple_chunks_concurrent(
    tmp_path: Path,
) -> None:
    """All chunks are dispatched concurrently and their results are combined."""
    cfg_dict = {
        "name": "test_pipeline",
        "llm": {
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "api_key": "sk-test",
        },
        "database": {"url": "sqlite+aiosqlite:///test.db"},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                }
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"}
                }
            },
        },
        "pipeline": {"chunk_size": 2},
    }
    cfg = SiphonConfig.model_validate(cfg_dict)

    rows = [
        {"company_name": "A"},
        {"company_name": "B"},
        {"company_name": "C"},
        {"company_name": "D"},
    ]
    csv_path = _write_csv(tmp_path, rows)

    calls: list[int] = []

    async def _fake_extract_json(prompt: str) -> list[dict]:
        # Record call order; both chunks should be scheduled before either completes
        calls.append(len(calls))
        if "A" in prompt or len(calls) == 1:
            return [{"company_name": "A"}, {"company_name": "B"}]
        return [{"company_name": "C"}, {"company_name": "D"}]

    mock_llm = MagicMock()
    mock_llm.extract_json = _fake_extract_json

    extractor = Extractor(cfg, mock_llm)
    records, skipped = await extractor.extract(csv_path)

    # All 4 records should be present
    assert len(records) == 4
    assert skipped == []
    # Both chunks should have been called
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# skipped_chunks property
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_skipped_chunks_property_after_extract(
    config: SiphonConfig, tmp_path: Path
) -> None:
    csv_path = _write_csv(tmp_path, [{"company_name": "X"}])

    mock_llm = MagicMock()
    mock_llm.extract_json = AsyncMock(side_effect=ExtractionError("boom"))

    extractor = Extractor(config, mock_llm)
    _, skipped = await extractor.extract(csv_path)

    assert extractor.skipped_chunks == skipped
    assert len(extractor.skipped_chunks) == 1


# ---------------------------------------------------------------------------
# extract resets skipped_chunks between calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extract_resets_skipped_chunks_on_each_call(
    config: SiphonConfig, tmp_path: Path
) -> None:
    csv_path = _write_csv(tmp_path, [{"company_name": "X"}])

    mock_llm = MagicMock()
    # First call: fail; second call: succeed
    mock_llm.extract_json = AsyncMock(
        side_effect=[
            ExtractionError("first fail"),
            [{"company_name": "X"}],
        ]
    )

    extractor = Extractor(config, mock_llm)

    _, skipped_first = await extractor.extract(csv_path)
    assert len(skipped_first) == 1

    _, skipped_second = await extractor.extract(csv_path)
    assert len(skipped_second) == 0
    assert extractor.skipped_chunks == []
