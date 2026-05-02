"""Tests for ReviewBatch — human-in-the-loop review API."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.reviewer import ReviewBatch, ReviewStatus


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
            "source": {"type": "spreadsheet"},
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
def batch(config):
    return ReviewBatch(records=list(SAMPLE_RECORDS), config=config)


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
    assert "revision_count" not in summary


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


def test_get_sql_preview_caps_at_five_records(config):
    many_records = [{"company_name": f"Corp {i}"} for i in range(10)]
    b = ReviewBatch(records=many_records, config=config)
    statements = b.get_sql_preview()
    assert len(statements) == 5


def test_get_sql_preview_uses_inserter_when_available(config):
    mock_inserter = MagicMock()
    mock_inserter.generate_sql_preview.return_value = ["INSERT INTO companies (name) VALUES ('X')"]
    b = ReviewBatch(
        records=SAMPLE_RECORDS,
        config=config,
        inserter=mock_inserter,
    )
    result = b.get_sql_preview()
    mock_inserter.generate_sql_preview.assert_called_once_with(SAMPLE_RECORDS)
    assert result == ["INSERT INTO companies (name) VALUES ('X')"]


# ---------------------------------------------------------------------------
# 7. get_summary() reflects multiple tables when config has multiple tables
# ---------------------------------------------------------------------------


def test_get_summary_multiple_tables():
    config2 = SiphonConfig.model_validate(
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
    b = ReviewBatch(records=[{"company_name": "Acme", "contact_email": None}], config=config2)
    summary = b.get_summary()
    assert sorted(summary["tables_affected"]) == ["companies", "contacts"]
