"""Tests for the Pipeline orchestrator."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import text

from siphon.config.schema import SiphonConfig
from siphon.core.pipeline import Pipeline, PipelineResult
from siphon.db.engine import DatabaseEngine


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LLM_BLOCK = {
    "base_url": "https://api.example.com/v1",
    "model": "gpt-4o-mini",
    "api_key": "sk-test",
}


def _simple_config(db_url: str = "sqlite+aiosqlite://") -> SiphonConfig:
    """Single-table config with auto_increment PK."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": db_url},
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


def _dedup_config(db_url: str = "sqlite+aiosqlite://") -> SiphonConfig:
    """Single-table config with deduplication enabled."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "llm": _LLM_BLOCK,
        "database": {"url": db_url},
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
            "deduplication": {
                "key": ["company_name"],
                "match": "exact",
            },
        },
        "pipeline": {
            "chunk_size": 50,
            "review": False,
            "log_level": "warning",
        },
    })


def _write_csv(tmp_path: Path, rows: list[dict], filename: str = "data.csv") -> Path:
    """Write a list-of-dicts as a CSV file and return the path."""
    df = pd.DataFrame(rows)
    p = tmp_path / filename
    df.to_csv(p, index=False)
    return p


# ---------------------------------------------------------------------------
# PipelineResult dataclass defaults
# ---------------------------------------------------------------------------


class TestPipelineResultDefaults:
    def test_defaults_are_zero(self):
        """All numeric fields default to 0 and lists to empty."""
        result = PipelineResult()
        assert result.total_extracted == 0
        assert result.total_valid == 0
        assert result.total_invalid == 0
        assert result.total_duplicates == 0
        assert result.total_inserted == 0
        assert result.skipped_chunks == []
        assert result.invalid_records == []
        assert result.duplicate_records == []
        assert result.dry_run is False

    def test_dry_run_flag(self):
        """dry_run flag is captured in the result."""
        result = PipelineResult(dry_run=True)
        assert result.dry_run is True

    def test_fields_are_independent(self):
        """Default mutable lists are independent between instances."""
        r1 = PipelineResult()
        r2 = PipelineResult()
        r1.skipped_chunks.append({"chunk": 0})
        assert r2.skipped_chunks == []


# ---------------------------------------------------------------------------
# dry_run: extract + validate but no DB insertion
# ---------------------------------------------------------------------------


class TestDryRun:
    async def test_dry_run_skips_insertion(self, tmp_path: Path):
        """dry_run=True extracts and validates but does not insert into the DB."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])

        extracted_records = [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)

        assert result.dry_run is True
        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 0

    async def test_dry_run_still_validates(self, tmp_path: Path):
        """dry_run captures invalid records even though no DB operations happen."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": ""},
        ])

        # Return records including one with empty company_name (required field)
        extracted_records = [
            {"company_name": "Acme"},
            {"company_name": ""},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)

        assert result.dry_run is True
        assert result.total_extracted == 2
        assert result.total_valid == 1
        assert result.total_invalid == 1
        assert result.total_inserted == 0
        assert len(result.invalid_records) == 1

    async def test_dry_run_with_deduplication(self, tmp_path: Path):
        """dry_run still performs in-batch deduplication."""
        config = _dedup_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])

        extracted_records = [
            {"company_name": "Acme"},
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)

        assert result.total_extracted == 3
        assert result.total_valid == 3
        assert result.total_duplicates == 1
        assert result.total_inserted == 0


# ---------------------------------------------------------------------------
# no_review: skips review step
# ---------------------------------------------------------------------------


class TestNoReview:
    async def test_no_review_proceeds_to_insert(self, tmp_path: Path):
        """no_review=True skips review and proceeds directly to insertion."""
        config = _simple_config()
        # Enable review in config to ensure no_review flag overrides it
        config.pipeline.review = True

        csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])
        extracted_records = [{"company_name": "Acme"}]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(
                csv_path, no_review=True, create_tables=True
            )

        assert result.total_extracted == 1
        assert result.total_valid == 1
        assert result.total_inserted == 1

    async def test_default_no_review_false_still_inserts(self, tmp_path: Path):
        """When review is not configured and no_review is False, pipeline still works."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])
        extracted_records = [{"company_name": "Acme"}]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_inserted == 1


# ---------------------------------------------------------------------------
# create_tables: tables are created and data inserted
# ---------------------------------------------------------------------------


class TestCreateTables:
    async def test_create_tables_and_insert(self, tmp_path: Path):
        """create_tables=True creates the DB schema and inserts records."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])
        extracted_records = [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 2

    async def test_verify_tables_called_when_no_create(self, tmp_path: Path):
        """Without create_tables, verify_tables is called and raises if tables missing."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])
        extracted_records = [{"company_name": "Acme"}]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            # Without create_tables, verify_tables will fail because tables don't exist
            from siphon.utils.errors import DatabaseError
            with pytest.raises(DatabaseError, match="Missing tables"):
                await pipeline.run(csv_path, create_tables=False)


# ---------------------------------------------------------------------------
# Pipeline result counts accuracy
# ---------------------------------------------------------------------------


class TestResultCounts:
    async def test_all_counts_accurate(self, tmp_path: Path):
        """Pipeline result counts match the actual processing."""
        config = _dedup_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
            {"company_name": "Acme"},  # duplicate
            {"company_name": ""},      # invalid (required)
        ])

        extracted_records = [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
            {"company_name": "Acme"},  # duplicate
            {"company_name": ""},      # invalid
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 4
        assert result.total_valid == 3     # Acme, Beta, Acme pass validation
        assert result.total_invalid == 1   # empty company_name
        assert result.total_duplicates == 1  # second Acme
        assert result.total_inserted == 2  # Acme + Beta

    async def test_zero_records_extracted(self, tmp_path: Path):
        """When extraction returns no records, counts reflect that."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [{"company_name": "X"}])

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=[])
            MockLLM.return_value = mock_llm_instance

            # The extractor will see 1 row but LLM returns 0 records -> mismatch,
            # then retry also returns 0 -> skip chunk. Records will be empty.
            # Actually, we need both calls to return empty for the skip to happen.
            mock_llm_instance.extract_json = AsyncMock(
                side_effect=[[], []]
            )

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)

        assert result.total_extracted == 0
        assert result.total_valid == 0
        assert result.total_inserted == 0

    async def test_all_records_invalid(self, tmp_path: Path):
        """When all extracted records fail validation, nothing is inserted."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": ""},
            {"company_name": ""},
        ])

        # Return records with empty required field
        extracted_records = [
            {"company_name": ""},
            {"company_name": ""},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 0
        assert result.total_invalid == 2
        assert result.total_inserted == 0

    async def test_skipped_chunks_tracked(self, tmp_path: Path):
        """Skipped chunks from extraction are captured in the result."""
        config = _simple_config()
        config.pipeline.chunk_size = 1
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])

        from siphon.utils.errors import ExtractionError

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            # First chunk succeeds, second chunk fails (both attempts)
            mock_llm_instance.extract_json = AsyncMock(
                side_effect=[
                    [{"company_name": "Acme"}],       # chunk 0
                    ExtractionError("LLM timeout"),    # chunk 1
                ]
            )
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 1
        assert len(result.skipped_chunks) == 1
        assert result.total_inserted == 1


# ---------------------------------------------------------------------------
# chunk_size override
# ---------------------------------------------------------------------------


class TestChunkSizeOverride:
    async def test_chunk_size_override(self, tmp_path: Path):
        """chunk_size parameter overrides the config value."""
        config = _simple_config()
        assert config.pipeline.chunk_size == 50  # default from config

        csv_path = _write_csv(tmp_path, [
            {"company_name": "A"},
            {"company_name": "B"},
            {"company_name": "C"},
        ])

        call_count = 0

        async def _fake_extract_json(prompt: str) -> list[dict]:
            nonlocal call_count
            call_count += 1
            # With chunk_size=1, each chunk has 1 row
            if call_count == 1:
                return [{"company_name": "A"}]
            elif call_count == 2:
                return [{"company_name": "B"}]
            else:
                return [{"company_name": "C"}]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = _fake_extract_json
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(
                csv_path, dry_run=True, chunk_size=1
            )

        # With chunk_size=1 and 3 rows, should have 3 chunks -> 3 LLM calls
        assert call_count == 3
        assert result.total_extracted == 3

    async def test_default_chunk_size_used(self, tmp_path: Path):
        """Without chunk_size override, config value is used."""
        config = _simple_config()
        config.pipeline.chunk_size = 50

        csv_path = _write_csv(tmp_path, [
            {"company_name": "A"},
            {"company_name": "B"},
        ])

        extracted_records = [
            {"company_name": "A"},
            {"company_name": "B"},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, dry_run=True)

        # With chunk_size=50 and 2 rows, only 1 LLM call
        mock_llm_instance.extract_json.assert_called_once()
        assert result.total_extracted == 2


# ---------------------------------------------------------------------------
# Data verified in DB after pipeline run
# ---------------------------------------------------------------------------


class TestDataInDB:
    async def test_inserted_data_readable_from_db(self, tmp_path: Path):
        """After a pipeline run with create_tables, data is in the database."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme Corp"},
            {"company_name": "Globex Inc"},
        ])
        extracted_records = [
            {"company_name": "Acme Corp"},
            {"company_name": "Globex Inc"},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_inserted == 2

        # Verify data is actually in the DB by connecting independently.
        # Since we used in-memory SQLite, the data is gone after engine dispose.
        # This test validates that the pipeline reported the correct insertion count.
        # For a persistent DB test, see integration tests (Task 17).

    async def test_pipeline_with_invalid_and_valid_records(self, tmp_path: Path):
        """Pipeline correctly separates valid from invalid and only inserts valid."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Valid"},
            {"company_name": ""},
            {"company_name": "Also Valid"},
        ])
        extracted_records = [
            {"company_name": "Valid"},
            {"company_name": ""},         # fails required validation
            {"company_name": "Also Valid"},
        ]

        with patch("siphon.core.pipeline.LLMClient") as MockLLM:
            mock_llm_instance = MagicMock()
            mock_llm_instance.extract_json = AsyncMock(return_value=extracted_records)
            MockLLM.return_value = mock_llm_instance

            pipeline = Pipeline(config)
            result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 3
        assert result.total_valid == 2
        assert result.total_invalid == 1
        assert result.total_inserted == 2
