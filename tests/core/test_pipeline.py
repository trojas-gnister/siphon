"""Tests for the v2 Pipeline orchestrator (source loading + mapping)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from siphon.config.schema import SiphonConfig
from siphon.core.pipeline import Pipeline, PipelineResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_config(db_url: str = "sqlite+aiosqlite://") -> SiphonConfig:
    """Single-table config with auto_increment PK — spreadsheet source."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "source": {"type": "spreadsheet"},
        "database": {"url": db_url},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "source": "company_name",
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


def _dedup_config(db_url: str = "sqlite+aiosqlite://") -> SiphonConfig:
    """Single-table config with deduplication enabled."""
    return SiphonConfig.model_validate({
        "name": "test_pipeline",
        "source": {"type": "spreadsheet"},
        "database": {"url": db_url},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "source": "company_name",
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
            "review": False,
            "log_level": "warning",
        },
    })


def _xml_config(db_url: str = "sqlite+aiosqlite://") -> SiphonConfig:
    """Single-table config for XML source."""
    return SiphonConfig.model_validate({
        "name": "test_xml_pipeline",
        "source": {
            "type": "xml",
            "root": "Companies.Company",
            "encoding": "utf-8",
        },
        "database": {"url": db_url},
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "source": "Name",
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


def _collection_config(db_url: str = "sqlite+aiosqlite://") -> SiphonConfig:
    """Config with a collection for nested data expansion."""
    return SiphonConfig.model_validate({
        "name": "test_collection_pipeline",
        "source": {
            "type": "xml",
            "root": "Cases.Case",
            "encoding": "utf-8",
            "force_list": ["Note"],
        },
        "database": {"url": db_url},
        "schema": {
            "fields": [
                {
                    "name": "case_code",
                    "type": "string",
                    "source": "Code",
                    "required": True,
                    "db": {"table": "cases", "column": "code"},
                },
            ],
            "collections": [
                {
                    "name": "notes",
                    "source_path": "Notes.Note",
                    "fields": [
                        {
                            "name": "note_text",
                            "type": "string",
                            "source": "Text",
                            "required": True,
                            "db": {"table": "case_notes", "column": "text"},
                        },
                    ],
                },
            ],
            "tables": {
                "cases": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
                "case_notes": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "pipeline": {
            "review": False,
            "log_level": "warning",
        },
    })


def _write_csv(
    tmp_path: Path, rows: list[dict], filename: str = "data.csv"
) -> Path:
    """Write a list-of-dicts as a CSV file and return the path."""
    df = pd.DataFrame(rows)
    p = tmp_path / filename
    df.to_csv(p, index=False)
    return p


def _write_xml(tmp_path: Path, xml_content: str, filename: str = "data.xml") -> Path:
    """Write XML content to a file and return the path."""
    p = tmp_path / filename
    p.write_text(xml_content, encoding="utf-8")
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
# Spreadsheet source: load CSV -> map -> validate -> insert
# ---------------------------------------------------------------------------


class TestSpreadsheetSource:
    async def test_csv_load_map_validate_insert(self, tmp_path: Path):
        """CSV records are loaded, mapped, validated, and inserted."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 2

    async def test_csv_with_mixed_valid_invalid(self, tmp_path: Path):
        """Pipeline correctly separates valid from invalid records."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Valid"},
            {"company_name": ""},
            {"company_name": "Also Valid"},
        ])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 3
        assert result.total_valid == 2
        assert result.total_invalid == 1
        assert result.total_inserted == 2


# ---------------------------------------------------------------------------
# XML source: load XML -> map -> validate -> insert
# ---------------------------------------------------------------------------


class TestXMLSource:
    async def test_xml_load_map_validate_insert(self, tmp_path: Path):
        """XML records are loaded, mapped, validated, and inserted."""
        config = _xml_config()
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Companies>
    <Company>
        <Name>Acme Corp</Name>
    </Company>
    <Company>
        <Name>Globex Inc</Name>
    </Company>
</Companies>"""
        xml_path = _write_xml(tmp_path, xml_content)

        pipeline = Pipeline(config)
        result = await pipeline.run(xml_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 2


# ---------------------------------------------------------------------------
# Dry run: load + map + validate but no DB insertion
# ---------------------------------------------------------------------------


class TestDryRun:
    async def test_dry_run_skips_insertion(self, tmp_path: Path):
        """dry_run=True loads, maps, and validates but does not insert."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, dry_run=True)

        assert result.dry_run is True
        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 0

    async def test_dry_run_still_validates(self, tmp_path: Path):
        """dry_run captures invalid records even without DB operations."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": ""},
        ])

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

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, dry_run=True)

        assert result.total_extracted == 3
        assert result.total_valid == 3
        assert result.total_duplicates == 1
        assert result.total_inserted == 0


# ---------------------------------------------------------------------------
# Create tables: tables are created and data inserted
# ---------------------------------------------------------------------------


class TestCreateTables:
    async def test_create_tables_and_insert(self, tmp_path: Path):
        """create_tables=True creates the DB schema and inserts records."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme"},
            {"company_name": "Beta"},
        ])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 2

    async def test_verify_tables_called_when_no_create(self, tmp_path: Path):
        """Without create_tables, verify_tables raises if tables are missing."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])

        pipeline = Pipeline(config)
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

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 4
        assert result.total_valid == 3     # Acme, Beta, Acme pass validation
        assert result.total_invalid == 1   # empty company_name
        assert result.total_duplicates == 1  # second Acme
        assert result.total_inserted == 2  # Acme + Beta

    async def test_zero_records_loaded(self, tmp_path: Path):
        """When source file has no data rows, counts reflect that."""
        config = _simple_config()
        # Write CSV with headers only (no data rows)
        p = tmp_path / "empty.csv"
        p.write_text("company_name\n")

        pipeline = Pipeline(config)
        result = await pipeline.run(p, dry_run=True)

        assert result.total_extracted == 0
        assert result.total_valid == 0
        assert result.total_inserted == 0

    async def test_all_records_invalid(self, tmp_path: Path):
        """When all records fail validation, nothing is inserted."""
        config = _simple_config()
        csv_path = _write_csv(tmp_path, [
            {"company_name": ""},
            {"company_name": ""},
        ])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 0
        assert result.total_invalid == 2
        assert result.total_inserted == 0


# ---------------------------------------------------------------------------
# Directory input: multiple CSVs aggregated
# ---------------------------------------------------------------------------


class TestDirectoryInput:
    async def test_multiple_csvs_aggregated(self, tmp_path: Path):
        """Multiple CSV files in a directory are loaded and aggregated."""
        config = _simple_config()
        _write_csv(tmp_path, [{"company_name": "Acme"}], "a.csv")
        _write_csv(tmp_path, [{"company_name": "Beta"}], "b.csv")

        pipeline = Pipeline(config)
        result = await pipeline.run(tmp_path, create_tables=True)

        assert result.total_extracted == 2
        assert result.total_valid == 2
        assert result.total_inserted == 2

    async def test_empty_directory(self, tmp_path: Path):
        """Empty directory returns early with zero counts."""
        config = _simple_config()
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        pipeline = Pipeline(config)
        result = await pipeline.run(empty_dir, dry_run=True)

        assert result.total_extracted == 0
        assert result.total_valid == 0
        assert result.total_inserted == 0


# ---------------------------------------------------------------------------
# Collections: nested data expanded into separate table rows
# ---------------------------------------------------------------------------


class TestCollections:
    async def test_collections_mapped(self, tmp_path: Path):
        """Collections from XML are expanded into collection records."""
        config = _collection_config()
        xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<Cases>
    <Case>
        <Code>CASE-001</Code>
        <Notes>
            <Note>
                <Text>First note</Text>
            </Note>
            <Note>
                <Text>Second note</Text>
            </Note>
        </Notes>
    </Case>
</Cases>"""
        xml_path = _write_xml(tmp_path, xml_content)

        pipeline = Pipeline(config)
        result = await pipeline.run(xml_path, create_tables=True)

        # Main records
        assert result.total_extracted == 1
        assert result.total_valid == 1
        assert result.total_inserted == 1


# ---------------------------------------------------------------------------
# No review by default: pipeline skips review when config.pipeline.review=False
# ---------------------------------------------------------------------------


class TestNoReview:
    async def test_review_false_skips_review(self, tmp_path: Path):
        """When review=False in config, pipeline skips review and inserts."""
        config = _simple_config()
        assert config.pipeline.review is False

        csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_extracted == 1
        assert result.total_valid == 1
        assert result.total_inserted == 1

    async def test_no_review_flag_overrides_config(self, tmp_path: Path):
        """no_review=True skips review even when config.pipeline.review=True."""
        config = _simple_config()
        config.pipeline.review = True

        csv_path = _write_csv(tmp_path, [{"company_name": "Acme"}])

        pipeline = Pipeline(config)
        result = await pipeline.run(
            csv_path, no_review=True, create_tables=True
        )

        assert result.total_extracted == 1
        assert result.total_valid == 1
        assert result.total_inserted == 1


# ---------------------------------------------------------------------------
# Data verified in DB after pipeline run
# ---------------------------------------------------------------------------


class TestDataInDB:
    async def test_persistent_db_data(self, tmp_path: Path):
        """After a pipeline run with a file-based DB, data is persisted."""
        db_path = tmp_path / "test.db"
        db_url = f"sqlite+aiosqlite:///{db_path}"
        config = _simple_config(db_url)
        csv_path = _write_csv(tmp_path, [
            {"company_name": "Acme Corp"},
            {"company_name": "Globex Inc"},
        ])

        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, create_tables=True)

        assert result.total_inserted == 2

        # Verify data by querying the DB independently
        from sqlalchemy import text
        from siphon.db.engine import DatabaseEngine

        engine = DatabaseEngine(config.database)
        try:
            async with engine.session() as session:
                rows = await session.execute(
                    text("SELECT name FROM companies ORDER BY name")
                )
                names = [row[0] for row in rows.fetchall()]
        finally:
            await engine.dispose()

        assert names == ["Acme Corp", "Globex Inc"]


class TestPipelineResultDiffField:
    def test_default_diff_is_none(self):
        from siphon.core.pipeline import PipelineResult
        result = PipelineResult()
        assert result.diff is None

    def test_diff_field_stores_dict(self):
        from siphon.core.pipeline import PipelineResult
        result = PipelineResult(diff={
            "insert": [], "update": [], "skip": [], "no_change": []
        })
        assert result.diff == {
            "insert": [], "update": [], "skip": [], "no_change": []
        }
