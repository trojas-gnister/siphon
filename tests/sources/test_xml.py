"""Tests for XMLLoader."""

from __future__ import annotations

from pathlib import Path

import pytest

from siphon.sources.base import SourceLoader
from siphon.sources.xml import XMLLoader
from siphon.utils.errors import SourceError

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
SAMPLE_XML = FIXTURES_DIR / "sample_incidents.xml"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def loader() -> XMLLoader:
    return XMLLoader(root="Cases.Case")


@pytest.fixture()
def loader_with_force_list() -> XMLLoader:
    return XMLLoader(root="Cases.Case", force_list=["Attachment", "Participant"])


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_xml_loader_satisfies_protocol(loader):
    """XMLLoader must be recognised as a SourceLoader via isinstance."""
    assert isinstance(loader, SourceLoader)


# ---------------------------------------------------------------------------
# Basic loading
# ---------------------------------------------------------------------------


def test_loads_xml_returns_list_of_dicts(loader):
    """load() returns a list of dicts with two cases."""
    records = loader.load(SAMPLE_XML)
    assert isinstance(records, list)
    assert all(isinstance(r, dict) for r in records)
    assert len(records) == 2


def test_records_have_expected_keys(loader):
    """Each dict has the expected top-level keys."""
    records = loader.load(SAMPLE_XML)
    first = records[0]
    expected_keys = {"CaseCode", "OrgName", "CaseStatus", "ReportNumber", "Details"}
    assert expected_keys.issubset(first.keys())


def test_first_record_values(loader):
    """First record contains correct field values."""
    records = loader.load(SAMPLE_XML)
    assert records[0]["CaseCode"] == "abc-123"
    assert records[0]["OrgName"] == "Acme Corp"
    assert records[0]["CaseStatus"] == "Closed"


def test_second_record_values(loader):
    """Second record contains correct field values."""
    records = loader.load(SAMPLE_XML)
    assert records[1]["CaseCode"] == "def-456"
    assert records[1]["OrgName"] == "Beta Inc"


# ---------------------------------------------------------------------------
# Root path navigation
# ---------------------------------------------------------------------------


def test_root_path_navigation(loader):
    """Dot-separated root path navigates correctly."""
    records = loader.load(SAMPLE_XML)
    # Successful navigation means we got Case records, not the raw Cases wrapper
    assert "CaseCode" in records[0]


def test_invalid_root_path_raises_source_error(tmp_path):
    """SourceError raised when root path is not present in XML."""
    xml_file = tmp_path / "data.xml"
    xml_file.write_text(
        '<?xml version="1.0"?><Root><Item><Id>1</Id></Item></Root>',
        encoding="utf-8",
    )
    loader = XMLLoader(root="Missing.Path")
    with pytest.raises(SourceError, match="Root path"):
        loader.load(xml_file)


# ---------------------------------------------------------------------------
# Nested collections
# ---------------------------------------------------------------------------


def test_nested_casenotes_preserved_as_list(loader):
    """CaseNotes.CaseNote is a list of note dicts when multiple notes exist."""
    records = loader.load(SAMPLE_XML)
    notes = records[0]["CaseNotes"]["CaseNote"]
    assert isinstance(notes, list)
    assert len(notes) == 2


def test_nested_participants_preserved_as_list(loader):
    """Participants.Participant is a list when multiple participants exist."""
    records = loader.load(SAMPLE_XML)
    participants = records[0]["Participants"]["Participant"]
    assert isinstance(participants, list)
    assert len(participants) == 2


# ---------------------------------------------------------------------------
# force_list
# ---------------------------------------------------------------------------


def test_force_list_makes_single_attachment_a_list(loader_with_force_list):
    """Attachment (only one in fixture) becomes a list when forced."""
    records = loader_with_force_list.load(SAMPLE_XML)
    attachments = records[0]["Attachments"]["Attachment"]
    assert isinstance(attachments, list)
    assert len(attachments) == 1
    assert attachments[0]["FileName"] == "report.pdf"


def test_force_list_does_not_break_multi_item_collections(loader_with_force_list):
    """force_list on Participant still yields all participants when multiple exist."""
    records = loader_with_force_list.load(SAMPLE_XML)
    participants = records[0]["Participants"]["Participant"]
    assert isinstance(participants, list)
    assert len(participants) == 2


def test_without_force_list_single_attachment_is_dict(loader):
    """Without force_list, a single Attachment is returned as a plain dict."""
    records = loader.load(SAMPLE_XML)
    attachment = records[0]["Attachments"]["Attachment"]
    assert isinstance(attachment, dict)


# ---------------------------------------------------------------------------
# Duplicate root elements
# ---------------------------------------------------------------------------


def test_duplicate_root_elements_uses_first_block(tmp_path):
    """When two <Cases> blocks are concatenated, only the first is parsed."""
    xml = (
        '<?xml version="1.0"?>'
        "<Cases><Case><Id>1</Id></Case></Cases>"
        "<Cases><Case><Id>2</Id></Case></Cases>"
    )
    xml_file = tmp_path / "dup.xml"
    xml_file.write_text(xml, encoding="utf-8")

    loader = XMLLoader(root="Cases.Case")
    records = loader.load(xml_file)
    assert len(records) == 1
    assert records[0]["Id"] == "1"


# ---------------------------------------------------------------------------
# Empty record list
# ---------------------------------------------------------------------------


def test_empty_element_list_returns_empty_list(tmp_path):
    """An empty container element returns an empty list."""
    xml = '<?xml version="1.0"?><Cases></Cases>'
    xml_file = tmp_path / "empty.xml"
    xml_file.write_text(xml, encoding="utf-8")

    loader = XMLLoader(root="Cases.Case")
    with pytest.raises(SourceError, match="Root path"):
        loader.load(xml_file)


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------


def test_missing_file_raises_source_error(tmp_path):
    """SourceError raised when the file does not exist."""
    loader = XMLLoader(root="Cases.Case")
    with pytest.raises(SourceError, match="File not found"):
        loader.load(tmp_path / "nonexistent.xml")


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def test_utf8_encoding_loads_correctly(tmp_path):
    """Loader correctly reads a UTF-8 encoded XML file."""
    xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        "<Cases><Case><Name>Ångström</Name></Case></Cases>"
    )
    xml_file = tmp_path / "utf8.xml"
    xml_file.write_bytes(xml.encode("utf-8"))

    loader = XMLLoader(root="Cases.Case", encoding="utf-8")
    records = loader.load(xml_file)
    assert len(records) == 1
    assert records[0]["Name"] == "Ångström"


def test_accepts_string_path(loader):
    """load() should accept a plain str path, not just pathlib.Path."""
    records = loader.load(str(SAMPLE_XML))
    assert len(records) == 2
