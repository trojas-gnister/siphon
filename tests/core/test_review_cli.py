"""Tests for ReviewCLI — Rich terminal renderer with interactive review loop."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from rich.console import Console

from siphon.config.schema import SiphonConfig
from siphon.core.review_cli import ReviewCLI
from siphon.core.reviewer import ReviewBatch, ReviewStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config() -> SiphonConfig:
    """Return a minimal SiphonConfig with one 'companies' table."""
    return SiphonConfig.model_validate(
        {
            "name": "test_pipeline",
            "source": {"type": "spreadsheet"},
            "database": {"url": "sqlite+aiosqlite:///test.db"},
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
        }
    )


SAMPLE_RECORDS = [
    {"company_name": "Acme Corp"},
    {"company_name": "Globex"},
]


def _make_batch(records: list[dict] | None = None) -> ReviewBatch:
    config = _make_config()
    return ReviewBatch(
        records=list(records if records is not None else SAMPLE_RECORDS),
        config=config,
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def console() -> Console:
    return Console(record=True, width=120)


@pytest.fixture
def review_cli(console: Console) -> ReviewCLI:
    return ReviewCLI(console=console)


# ---------------------------------------------------------------------------
# 1. _display_batch renders summary panel with record count and tables
# ---------------------------------------------------------------------------


def test_display_batch_renders_summary_panel(review_cli, console):
    batch = _make_batch()
    review_cli._display_batch(batch)
    output = console.export_text()

    assert "2" in output          # record count
    assert "companies" in output  # table name
    assert "pending" in output    # status


# ---------------------------------------------------------------------------
# 2. _display_batch renders records table with column headers
# ---------------------------------------------------------------------------


def test_display_batch_renders_records_table(review_cli, console):
    batch = _make_batch()
    review_cli._display_batch(batch)
    output = console.export_text()

    assert "company_name" in output  # column header
    assert "Acme Corp" in output
    assert "Globex" in output


# ---------------------------------------------------------------------------
# 3. _display_batch renders SQL preview tree
# ---------------------------------------------------------------------------


def test_display_batch_renders_sql_preview(review_cli, console):
    batch = _make_batch()
    review_cli._display_batch(batch)
    output = console.export_text()

    assert "SQL Preview" in output
    assert "INSERT INTO companies" in output


# ---------------------------------------------------------------------------
# 4. _display_batch handles empty records gracefully
# ---------------------------------------------------------------------------


def test_display_batch_handles_empty_records(review_cli, console):
    batch = _make_batch(records=[])
    review_cli._display_batch(batch)
    output = console.export_text()

    # Summary panel still rendered
    assert "Review Batch" in output
    # No records table row content — "0" should appear in record count
    assert "0" in output


# ---------------------------------------------------------------------------
# 5. run_review with "approve" ends loop with APPROVED status
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_approve_ends_loop(mock_ask, review_cli):
    mock_ask.return_value = "approve"
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.APPROVED
    mock_ask.assert_called_once()


# ---------------------------------------------------------------------------
# 6. run_review with "a" shortcut approves the batch
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_approve_shortcut_ends_loop(mock_ask, review_cli):
    mock_ask.return_value = "a"
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.APPROVED


# ---------------------------------------------------------------------------
# 7. run_review with "reject" ends loop with REJECTED status
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_reject_ends_loop(mock_ask, review_cli):
    mock_ask.return_value = "reject"
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.REJECTED
    mock_ask.assert_called_once()


# ---------------------------------------------------------------------------
# 8. run_review with "r" shortcut rejects the batch
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_reject_shortcut_ends_loop(mock_ask, review_cli):
    mock_ask.return_value = "r"
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.REJECTED


# ---------------------------------------------------------------------------
# 9. run_review with unknown input prints error and stays in loop
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_unknown_action_prints_error_and_continues(mock_ask, review_cli, console):
    # First input is unknown, second approves
    mock_ask.side_effect = ["bad command", "approve"]
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.APPROVED
    assert mock_ask.call_count == 2
    output = console.export_text()
    assert "Unknown action" in output


# ---------------------------------------------------------------------------
# 10. _display_batch does not show revision count
# ---------------------------------------------------------------------------


def test_display_batch_no_revision_count(review_cli, console):
    batch = _make_batch()
    review_cli._display_batch(batch)
    output = console.export_text()

    assert "Revisions" not in output
    assert "revision" not in output.lower()


# ---------------------------------------------------------------------------
# 11. _display_batch truncates records table at 10 rows with ellipsis row
# ---------------------------------------------------------------------------


def test_display_batch_truncates_at_10_records(review_cli, console):
    many_records = [{"company_name": f"Corp {i}"} for i in range(15)]
    batch = _make_batch(records=many_records)
    review_cli._display_batch(batch)
    output = console.export_text()

    # The ellipsis row should be present
    assert "..." in output


# ---------------------------------------------------------------------------
# 12. _display_batch SQL preview truncates at 5 statements with "more" note
# ---------------------------------------------------------------------------


def test_display_batch_sql_preview_more_note(review_cli, console):
    # Build a batch with enough records (6+) so get_sql_preview returns > 5
    # The fallback path caps at 5, so we mock get_sql_preview instead
    batch = _make_batch()
    batch.get_sql_preview = MagicMock(  # type: ignore[method-assign]
        return_value=[f"INSERT INTO companies (name) VALUES ('Corp {i}')" for i in range(8)]
    )

    review_cli._display_batch(batch)
    output = console.export_text()

    assert "more" in output


# ---------------------------------------------------------------------------
# 13. ReviewCLI uses default Console when none is provided
# ---------------------------------------------------------------------------


def test_review_cli_default_console():
    cli = ReviewCLI()
    assert cli._console is not None
    assert isinstance(cli._console, Console)


# ---------------------------------------------------------------------------
# 14. run_review approves on "APPROVE" (case-insensitive)
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_approve_case_insensitive(mock_ask, review_cli):
    mock_ask.return_value = "APPROVE"
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.APPROVED


# ---------------------------------------------------------------------------
# 15. run_review rejects on "REJECT" (case-insensitive)
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_reject_case_insensitive(mock_ask, review_cli):
    mock_ask.return_value = "REJECT"
    batch = _make_batch()

    result = await review_cli.run_review(batch)

    assert result.status == ReviewStatus.REJECTED


# ---------------------------------------------------------------------------
# 16. run_review prompt text does not mention revision
# ---------------------------------------------------------------------------


@patch("siphon.core.review_cli.Prompt.ask")
async def test_prompt_text_no_revision_mention(mock_ask, review_cli, console):
    mock_ask.return_value = "approve"
    batch = _make_batch()

    await review_cli.run_review(batch)

    output = console.export_text()
    assert "revision" not in output.lower()
