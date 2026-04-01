"""End-to-end integration tests replicating the example use case.

Verifies the full pipeline: extract -> validate -> dedup -> insert,
including multi-table insertion, parent FK resolution, junction rows,
and deduplication, all against a real (file-based) SQLite database.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# Mock LLM response: what the LLM would extract from the CSV.
# This is the "normalised" output that an LLM would produce from the raw CSV.
MOCK_LLM_RESPONSE = [
    {"company_name": "Acme Corp", "parent_entity": "", "phone": "5551234567", "website": "https://acme.com", "address": "123 Main St", "state": "CA"},
    {"company_name": "Acme West", "parent_entity": "Acme Corp", "phone": "5559876543", "website": "https://acmewest.com", "address": "456 Oak Ave", "state": "CA"},
    {"company_name": "Acme East", "parent_entity": "Acme Corp", "phone": "5551112222", "website": "https://acmeeast.com", "address": "789 Pine Rd", "state": "NY"},
    {"company_name": "Beta Inc", "parent_entity": "", "phone": "5553334444", "website": "https://beta.io", "address": "321 Elm St", "state": "TX"},
    {"company_name": "Beta South", "parent_entity": "Beta Inc", "phone": "5554445555", "website": "https://betasouth.com", "address": "654 Maple Dr", "state": "FL"},
    {"company_name": "Gamma LLC", "parent_entity": "", "phone": "5556667777", "website": "https://gamma.org", "address": "987 Cedar Ln", "state": "WA"},
    {"company_name": "Delta Corp", "parent_entity": "", "phone": "5557778888", "website": "https://delta.com", "address": "111 Birch St", "state": "OR"},
    {"company_name": "Acme Corp", "parent_entity": "", "phone": "5551234567", "website": "https://acme.com", "address": "123 Main St", "state": "CA"},
    {"company_name": "Epsilon", "parent_entity": "", "phone": "5559990000", "website": "https://epsilon.net", "address": "222 Spruce Ave", "state": "IL"},
    {"company_name": "Acme Central", "parent_entity": "Acme Corp", "phone": "5551234000", "website": "https://acmecentral.com", "address": "333 Walnut Blvd", "state": "OH"},
]


def _load_config_with_db(tmp_path: Path):
    """Load the example config and override DB URL to a file-based SQLite in tmp_path."""
    config = load_config(FIXTURES_DIR / "example_config.yaml")
    db_path = tmp_path / "test.db"
    config.database.url = f"sqlite+aiosqlite:///{db_path}"
    return config


async def _run_pipeline(config, mock_response=None):
    """Run the pipeline with a mocked LLM client.

    Returns the PipelineResult.
    """
    if mock_response is None:
        mock_response = MOCK_LLM_RESPONSE

    with patch("siphon.core.pipeline.LLMClient") as MockLLMClient:
        mock_instance = MagicMock()
        mock_instance.extract_json = AsyncMock(return_value=mock_response)
        MockLLMClient.return_value = mock_instance

        pipeline = Pipeline(config)
        result = await pipeline.run(
            FIXTURES_DIR / "sample_companies.csv",
            create_tables=True,
            no_review=True,
        )

    return result


async def _query_db(db_url: str, sql: str) -> list:
    """Execute a raw SQL query against the test database and return rows."""
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            return result.fetchall()
    finally:
        await engine.dispose()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullPipeline:
    """Full pipeline: extract, validate, dedup, insert, verify DB state."""

    async def test_pipeline_result_counts(self, tmp_path: Path):
        """Pipeline returns correct counts for extracted, duplicates, inserted."""
        config = _load_config_with_db(tmp_path)
        result = await _run_pipeline(config)

        assert result.total_extracted == 10  # 10 rows from CSV
        assert result.total_duplicates == 1  # "Acme Corp" duplicate
        assert result.total_inserted == 9    # 10 - 1 duplicate
        assert result.total_invalid == 0     # all should validate

    async def test_companies_table_populated(self, tmp_path: Path):
        """The companies table has 9 rows (8 unique companies + 1 deduped)."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(config.database.url, "SELECT * FROM companies")
        assert len(rows) == 9

    async def test_company_names_correct(self, tmp_path: Path):
        """All 9 unique company names are present in the companies table."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(
            config.database.url, "SELECT name FROM companies ORDER BY name"
        )
        names = [row[0] for row in rows]
        expected = sorted([
            "Acme Central", "Acme Corp", "Acme East", "Acme West",
            "Beta Inc", "Beta South", "Delta Corp", "Epsilon", "Gamma LLC",
        ])
        assert names == expected

    async def test_addresses_table_populated(self, tmp_path: Path):
        """The addresses table has 9 rows (one per non-duplicate record)."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(config.database.url, "SELECT * FROM addresses")
        assert len(rows) == 9


class TestDeduplication:
    """Deduplication removes the duplicate 'Acme Corp' row."""

    async def test_duplicate_count(self, tmp_path: Path):
        """Exactly 1 duplicate is detected."""
        config = _load_config_with_db(tmp_path)
        result = await _run_pipeline(config)
        assert result.total_duplicates == 1

    async def test_duplicate_record_is_acme(self, tmp_path: Path):
        """The duplicate record is the second 'Acme Corp' entry."""
        config = _load_config_with_db(tmp_path)
        result = await _run_pipeline(config)
        assert len(result.duplicate_records) == 1
        assert result.duplicate_records[0]["company_name"].lower() == "acme corp"

    async def test_no_duplicate_rows_in_db(self, tmp_path: Path):
        """The companies table has no duplicate names."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(
            config.database.url,
            "SELECT name, COUNT(*) as cnt FROM companies GROUP BY name HAVING cnt > 1",
        )
        assert len(rows) == 0


class TestParentFKResolution:
    """Children (Acme West, Acme East, Acme Central, Beta South) have parent_id set."""

    async def test_children_have_parent_id(self, tmp_path: Path):
        """Child companies have a non-null parent_id."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(
            config.database.url,
            "SELECT name, parent_id FROM companies WHERE parent_id IS NOT NULL",
        )
        child_names = sorted([row[0] for row in rows])
        assert child_names == ["Acme Central", "Acme East", "Acme West", "Beta South"]

    async def test_parent_id_points_to_correct_parent(self, tmp_path: Path):
        """Acme West's parent_id resolves to Acme Corp's id."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(
            config.database.url,
            """
            SELECT child.name, parent.name
            FROM companies child
            JOIN companies parent ON child.parent_id = parent.id
            ORDER BY child.name
            """,
        )
        child_parent_map = {row[0]: row[1] for row in rows}
        assert child_parent_map["Acme West"] == "Acme Corp"
        assert child_parent_map["Acme East"] == "Acme Corp"
        assert child_parent_map["Acme Central"] == "Acme Corp"
        assert child_parent_map["Beta South"] == "Beta Inc"

    async def test_root_companies_have_no_parent(self, tmp_path: Path):
        """Root companies (Acme Corp, Beta Inc, etc.) have NULL parent_id."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(
            config.database.url,
            "SELECT name FROM companies WHERE parent_id IS NULL ORDER BY name",
        )
        root_names = [row[0] for row in rows]
        expected = sorted(["Acme Corp", "Beta Inc", "Delta Corp", "Epsilon", "Gamma LLC"])
        assert root_names == expected


class TestJunctionRows:
    """Each non-duplicate record should have a junction row in company_addresses."""

    async def test_junction_table_row_count(self, tmp_path: Path):
        """The company_addresses table has 9 rows (one per inserted record)."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        rows = await _query_db(
            config.database.url, "SELECT * FROM company_addresses"
        )
        assert len(rows) == 9

    async def test_junction_links_valid_ids(self, tmp_path: Path):
        """Every junction row links to existing company and address IDs."""
        config = _load_config_with_db(tmp_path)
        await _run_pipeline(config)

        # Verify all company_id values exist in companies
        rows = await _query_db(
            config.database.url,
            """
            SELECT ca.company_id
            FROM company_addresses ca
            LEFT JOIN companies c ON ca.company_id = c.id
            WHERE c.id IS NULL
            """,
        )
        assert len(rows) == 0, "Found junction rows with invalid company_id"

        # Verify all address_id values exist in addresses
        rows = await _query_db(
            config.database.url,
            """
            SELECT ca.address_id
            FROM company_addresses ca
            LEFT JOIN addresses a ON ca.address_id = a.id
            WHERE a.id IS NULL
            """,
        )
        assert len(rows) == 0, "Found junction rows with invalid address_id"


class TestDryRun:
    """Dry run should not insert anything into the database."""

    async def test_dry_run_no_insertion(self, tmp_path: Path):
        """Dry run returns counts but total_inserted is 0."""
        config = _load_config_with_db(tmp_path)

        with patch("siphon.core.pipeline.LLMClient") as MockLLMClient:
            mock_instance = MagicMock()
            mock_instance.extract_json = AsyncMock(return_value=MOCK_LLM_RESPONSE)
            MockLLMClient.return_value = mock_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(
                FIXTURES_DIR / "sample_companies.csv",
                dry_run=True,
                no_review=True,
            )

        assert result.dry_run is True
        assert result.total_extracted == 10
        assert result.total_inserted == 0
        assert result.total_duplicates == 1
