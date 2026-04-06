"""Tests for SpreadsheetLoader."""

from __future__ import annotations

import pytest
import openpyxl

from siphon.sources.base import SourceLoader
from siphon.sources.spreadsheet import SpreadsheetLoader
from siphon.utils.errors import SourceError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def loader() -> SpreadsheetLoader:
    return SpreadsheetLoader()


@pytest.fixture()
def csv_file(tmp_path):
    """A simple two-row CSV file."""
    f = tmp_path / "data.csv"
    f.write_text("name,age,city\nAlice,30,London\nBob,25,Paris\n")
    return f


@pytest.fixture()
def csv_with_nan(tmp_path):
    """A CSV file that has a missing value in one cell."""
    f = tmp_path / "nan.csv"
    f.write_text("name,age,city\nAlice,,London\nBob,25,\n")
    return f


@pytest.fixture()
def empty_csv(tmp_path):
    """A CSV with a header row but no data rows."""
    f = tmp_path / "empty.csv"
    f.write_text("name,age,city\n")
    return f


@pytest.fixture()
def xlsx_file(tmp_path):
    """An XLSX workbook with two sheets."""
    wb = openpyxl.Workbook()
    ws1 = wb.active
    ws1.title = "Sheet1"
    ws1.append(["product", "price"])
    ws1.append(["Widget", "9.99"])
    ws1.append(["Gadget", "14.99"])

    ws2 = wb.create_sheet("Sheet2")
    ws2.append(["country", "code"])
    ws2.append(["Norway", "NO"])

    path = tmp_path / "workbook.xlsx"
    wb.save(path)
    return path


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_spreadsheet_loader_satisfies_protocol(loader):
    """SpreadsheetLoader must be recognised as a SourceLoader via isinstance."""
    assert isinstance(loader, SourceLoader)


# ---------------------------------------------------------------------------
# CSV loading
# ---------------------------------------------------------------------------


def test_csv_returns_list_of_dicts(loader, csv_file):
    records = loader.load(csv_file)
    assert isinstance(records, list)
    assert all(isinstance(r, dict) for r in records)


def test_csv_correct_values(loader, csv_file):
    records = loader.load(csv_file)
    assert len(records) == 2
    assert records[0] == {"name": "Alice", "age": "30", "city": "London"}
    assert records[1] == {"name": "Bob", "age": "25", "city": "Paris"}


def test_csv_keys_match_column_names(loader, csv_file):
    records = loader.load(csv_file)
    assert set(records[0].keys()) == {"name", "age", "city"}


def test_csv_nan_filled_as_empty_string(loader, csv_with_nan):
    records = loader.load(csv_with_nan)
    assert records[0]["age"] == ""
    assert records[1]["city"] == ""


def test_empty_csv_returns_empty_list(loader, empty_csv):
    records = loader.load(empty_csv)
    assert records == []


# ---------------------------------------------------------------------------
# XLSX loading
# ---------------------------------------------------------------------------


def test_xlsx_default_sheet(loader, xlsx_file):
    """Default load returns first sheet."""
    records = loader.load(xlsx_file)
    assert len(records) == 2
    assert records[0] == {"product": "Widget", "price": "9.99"}
    assert records[1] == {"product": "Gadget", "price": "14.99"}


def test_xlsx_keys_match_column_names(loader, xlsx_file):
    records = loader.load(xlsx_file)
    assert set(records[0].keys()) == {"product", "price"}


def test_xlsx_sheet_by_name(loader, xlsx_file):
    """sheet= parameter selects a sheet by name."""
    records = loader.load(xlsx_file, sheet="Sheet2")
    assert len(records) == 1
    assert records[0] == {"country": "Norway", "code": "NO"}


def test_xlsx_sheet_by_index(loader, xlsx_file):
    """sheet= parameter selects a sheet by 0-based index."""
    records = loader.load(xlsx_file, sheet=1)
    assert len(records) == 1
    assert records[0]["country"] == "Norway"


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_unsupported_format_raises_source_error(loader, tmp_path):
    f = tmp_path / "data.txt"
    f.write_text("hello\n")
    with pytest.raises(SourceError, match="Unsupported file format"):
        loader.load(f)


def test_missing_file_raises_source_error(loader, tmp_path):
    missing = tmp_path / "does_not_exist.csv"
    with pytest.raises(SourceError, match="Failed to read"):
        loader.load(missing)


def test_accepts_string_path(loader, csv_file):
    """load() should accept a plain str path, not just pathlib.Path."""
    records = loader.load(str(csv_file))
    assert len(records) == 2
