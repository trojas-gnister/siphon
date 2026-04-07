"""Integration test for XML source with custom transforms and collections."""

import pytest
from pathlib import Path
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_incident_config(tmp_path: Path):
    config = load_config(FIXTURES_DIR / "incident_config.yaml")
    db_path = tmp_path / "incidents.db"
    config.database.url = f"sqlite+aiosqlite:///{db_path}"
    # Fix transforms file path (relative to config file)
    config.transforms.file = str(FIXTURES_DIR / "incident_transforms.py")
    return config


async def _run_pipeline(config):
    pipeline = Pipeline(config)
    result = await pipeline.run(
        FIXTURES_DIR / "sample_incidents.xml",
        create_tables=True,
        no_review=True,
    )
    return result


async def _query(db_url, sql):
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            r = await conn.execute(text(sql))
            return r.fetchall()
    finally:
        await engine.dispose()


class TestIncidentPipeline:
    async def test_pipeline_result_counts(self, tmp_path):
        config = _load_incident_config(tmp_path)
        result = await _run_pipeline(config)
        assert result.total_extracted == 2  # 2 cases in XML
        assert result.total_valid == 2
        assert result.total_invalid == 0
        assert result.total_inserted == 2

    async def test_incidents_table_populated(self, tmp_path):
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url, "SELECT * FROM incidents")
        assert len(rows) == 2

    async def test_reference_number_template(self, tmp_path):
        """reference_number should be '{prefix}-{CaseCode}'"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT reference_number FROM incidents ORDER BY reference_number")
        refs = [r[0] for r in rows]
        assert "TEST-abc-123" in refs
        assert "TEST-def-456" in refs

    async def test_status_map_transform(self, tmp_path):
        """CaseStatus 'Closed' should map to 8, 'In Process' to 6"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT reference_number, incident_status_id FROM incidents ORDER BY reference_number")
        status_by_ref = {r[0]: r[1] for r in rows}
        assert status_by_ref["TEST-abc-123"] == 8  # Closed
        assert status_by_ref["TEST-def-456"] == 6  # In Process

    async def test_custom_work_location(self, tmp_path):
        """work_location should concatenate address components"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT work_location FROM incidents WHERE reference_number = 'TEST-abc-123'")
        loc = rows[0][0]
        assert "123 Main St" in loc
        assert "Springfield" in loc
        assert "IL" in loc

    async def test_constant_value_field(self, tmp_path):
        """is_data_import should be True for all records"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT is_data_import FROM incidents")
        assert all(r[0] == 1 for r in rows)  # SQLite stores bool as 1/0

    async def test_uuid_generated(self, tmp_path):
        """Each incident should have a unique UUID"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT uuid FROM incidents")
        uuids = [r[0] for r in rows]
        assert len(uuids) == 2
        assert len(set(uuids)) == 2  # all unique
        assert all(len(u) == 36 for u in uuids)  # UUID format


class TestCollectionExpansion:
    async def test_notes_table_populated(self, tmp_path):
        """Case notes should be expanded into incident_notes table"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT * FROM incident_notes")
        # Case 1 has 2 notes, Case 2 has 0 notes = 2 total
        assert len(rows) == 2

    async def test_note_content(self, tmp_path):
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT note FROM incident_notes ORDER BY note")
        notes = [r[0] for r in rows]
        assert "First note text" in notes
        assert "Second note text" in notes

    async def test_reverse_name_transform(self, tmp_path):
        """UserName 'Smith, John' should become 'John Smith'"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT original_created_by FROM incident_notes ORDER BY original_created_by")
        names = [r[0] for r in rows]
        assert "John Smith" in names
        assert "Jane Doe" in names

    async def test_notes_have_parent_reference(self, tmp_path):
        """Each note should have the parent's reference_number"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT reference_number FROM incident_notes")
        refs = [r[0] for r in rows]
        assert all(r == "TEST-abc-123" for r in refs)  # Both notes from case 1


class TestFKResolution:
    async def test_notes_fk_resolved(self, tmp_path):
        """incident_notes.incident_id should reference incidents.id"""
        config = _load_incident_config(tmp_path)
        await _run_pipeline(config)
        rows = await _query(config.database.url,
            "SELECT incident_id FROM incident_notes")
        fks = [r[0] for r in rows]
        # All should be non-null and reference the first incident
        assert all(fk is not None for fk in fks)

        # Verify the FK points to the correct incident
        incident_rows = await _query(config.database.url,
            "SELECT id FROM incidents WHERE reference_number = 'TEST-abc-123'")
        expected_id = incident_rows[0][0]
        assert all(fk == expected_id for fk in fks)
