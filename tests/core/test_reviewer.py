"""Tests for ReviewBatch — human-in-the-loop review API."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.reviewer import ReviewBatch, ReviewStatus
from siphon.utils.errors import ReviewError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(extra_fields: list[dict] | None = None) -> SiphonConfig:
    """Return a minimal SiphonConfig with one 'companies' table."""
    fields = [
        {
            "name": "company_name",
            "type": "string",
            "required": True,
            "db": {"table": "companies", "column": "name"},
        },
    ]
    if extra_fields:
        fields.extend(extra_fields)

    return SiphonConfig.model_validate(
        {
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com",
                "model": "gpt-4o-mini",
                "api_key": "sk-test",
            },
            "database": {"url": "sqlite+aiosqlite:///test.db"},
            "schema": {
                "fields": fields,
                "tables": {
                    "companies": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                },
            },
        }
    )


def _make_llm_client(return_value: list[dict] | None = None) -> MagicMock:
    """Return a mock LLMClient whose extract_json returns *return_value*."""
    client = MagicMock()
    client.extract_json = AsyncMock(return_value=return_value or [])
    return client


SAMPLE_RECORDS = [
    {"company_name": "Acme Corp"},
    {"company_name": "Globex"},
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def config():
    return _make_config()


@pytest.fixture
def llm_client():
    return _make_llm_client()


@pytest.fixture
def batch(config, llm_client):
    return ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=llm_client, config=config)


# ---------------------------------------------------------------------------
# 1. Initial status is PENDING
# ---------------------------------------------------------------------------


def test_initial_status_is_pending(batch):
    assert batch.status == ReviewStatus.PENDING


# ---------------------------------------------------------------------------
# 2. records property returns the records
# ---------------------------------------------------------------------------


def test_records_property(batch):
    assert batch.records == SAMPLE_RECORDS


# ---------------------------------------------------------------------------
# 3. approve() sets status to APPROVED
# ---------------------------------------------------------------------------


def test_approve_sets_status(batch):
    batch.approve()
    assert batch.status == ReviewStatus.APPROVED


# ---------------------------------------------------------------------------
# 4. reject() sets status to REJECTED
# ---------------------------------------------------------------------------


def test_reject_sets_status(batch):
    batch.reject()
    assert batch.status == ReviewStatus.REJECTED


# ---------------------------------------------------------------------------
# 5. get_summary() returns correct counts
# ---------------------------------------------------------------------------


def test_get_summary_pending(batch):
    summary = batch.get_summary()
    assert summary["record_count"] == 2
    assert summary["tables_affected"] == ["companies"]
    assert summary["status"] == "pending"
    assert summary["revision_count"] == 0


def test_get_summary_after_approve(batch):
    batch.approve()
    summary = batch.get_summary()
    assert summary["status"] == "approved"


def test_get_summary_after_reject(batch):
    batch.reject()
    summary = batch.get_summary()
    assert summary["status"] == "rejected"


# ---------------------------------------------------------------------------
# 6. get_sql_preview() returns INSERT statements (fallback path)
# ---------------------------------------------------------------------------


def test_get_sql_preview_returns_inserts(batch):
    statements = batch.get_sql_preview()
    assert len(statements) == 2
    for stmt in statements:
        assert stmt.startswith("INSERT INTO companies")
        assert "name" in stmt


def test_get_sql_preview_caps_at_five_records(config, llm_client):
    many_records = [{"company_name": f"Corp {i}"} for i in range(10)]
    b = ReviewBatch(records=many_records, llm_client=llm_client, config=config)
    statements = b.get_sql_preview()
    assert len(statements) == 5


def test_get_sql_preview_uses_inserter_when_available(config, llm_client):
    mock_inserter = MagicMock()
    mock_inserter.generate_sql_preview.return_value = ["INSERT INTO companies (name) VALUES ('X')"]
    b = ReviewBatch(
        records=SAMPLE_RECORDS,
        llm_client=llm_client,
        config=config,
        inserter=mock_inserter,
    )
    result = b.get_sql_preview()
    mock_inserter.generate_sql_preview.assert_called_once_with(SAMPLE_RECORDS)
    assert result == ["INSERT INTO companies (name) VALUES ('X')"]


# ---------------------------------------------------------------------------
# 7. revise() calls LLM with revision prompt, re-validates, returns new batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revise_returns_new_batch(config):
    revised_data = [{"company_name": "New Corp"}]
    client = _make_llm_client(return_value=revised_data)
    original = ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=client, config=config)

    new_batch = await original.revise("rename first company to New Corp")

    assert isinstance(new_batch, ReviewBatch)
    # LLM was called once
    client.extract_json.assert_awaited_once()
    # New batch contains the valid revised record
    assert new_batch.records == [{"company_name": "New Corp"}]


@pytest.mark.asyncio
async def test_revise_does_not_mutate_original(config):
    revised_data = [{"company_name": "Changed"}]
    client = _make_llm_client(return_value=revised_data)
    original = ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=client, config=config)

    await original.revise("change names")

    # Original batch records are unchanged
    assert original.records == SAMPLE_RECORDS


# ---------------------------------------------------------------------------
# 8. revise() increments revision_count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revise_increments_revision_count(config):
    client = _make_llm_client(return_value=[{"company_name": "Rev 1"}])
    original = ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=client, config=config)
    assert original.revision_count == 0

    new_batch = await original.revise("first revision")
    assert new_batch.revision_count == 1


# ---------------------------------------------------------------------------
# 9. Multiple revision rounds work (revise twice)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revise_multiple_rounds(config):
    client = MagicMock()
    client.extract_json = AsyncMock(
        side_effect=[
            [{"company_name": "Round 1"}],
            [{"company_name": "Round 2"}],
        ]
    )
    original = ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=client, config=config)

    batch_1 = await original.revise("first revision")
    batch_2 = await batch_1.revise("second revision")

    assert batch_1.revision_count == 1
    assert batch_2.revision_count == 2
    assert batch_2.records == [{"company_name": "Round 2"}]


# ---------------------------------------------------------------------------
# 10. revise() raises ReviewError on LLM failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revise_raises_review_error_on_llm_failure(config):
    client = MagicMock()
    client.extract_json = AsyncMock(side_effect=RuntimeError("API timeout"))
    b = ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=client, config=config)

    with pytest.raises(ReviewError, match="Revision failed"):
        await b.revise("do something")


# ---------------------------------------------------------------------------
# 11. revise() with partially invalid records: only valid ones in new batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_revise_filters_invalid_records(config):
    # One valid, one invalid (missing required company_name)
    mixed_records = [
        {"company_name": "Valid Corp"},
        {"company_name": ""},  # empty string → None → fails required validation
    ]
    client = _make_llm_client(return_value=mixed_records)
    original = ReviewBatch(records=list(SAMPLE_RECORDS), llm_client=client, config=config)

    new_batch = await original.revise("introduce an invalid record")

    # Only the valid record should be in the new batch
    assert len(new_batch.records) == 1
    assert new_batch.records[0]["company_name"] == "Valid Corp"


# ---------------------------------------------------------------------------
# 12. get_summary() reflects multiple tables when config has multiple tables
# ---------------------------------------------------------------------------


def test_get_summary_multiple_tables():
    config2 = SiphonConfig.model_validate(
        {
            "name": "test_pipeline",
            "llm": {
                "base_url": "https://api.example.com",
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
                    },
                    {
                        "name": "contact_email",
                        "type": "email",
                        "required": False,
                        "db": {"table": "contacts", "column": "email"},
                    },
                ],
                "tables": {
                    "companies": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                    "contacts": {
                        "primary_key": {"column": "id", "type": "auto_increment"},
                    },
                },
            },
        }
    )
    client = _make_llm_client()
    b = ReviewBatch(records=[{"company_name": "Acme", "contact_email": None}], llm_client=client, config=config2)
    summary = b.get_summary()
    assert sorted(summary["tables_affected"]) == ["companies", "contacts"]
