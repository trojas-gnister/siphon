"""End-to-end test for the upsert flow."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_config(tmp_path: Path):
    config = load_config(FIXTURES_DIR / "companies_upsert_config.yaml")
    db_path = tmp_path / "test.db"
    config.database.url = f"sqlite+aiosqlite:///{db_path}"
    return config


async def _run(config, csv_name: str):
    pipeline = Pipeline(config)
    return await pipeline.run(
        FIXTURES_DIR / csv_name,
        create_tables=True,
        no_review=True,
    )


async def _query(db_url: str, sql: str):
    engine = create_async_engine(db_url)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            return result.fetchall()
    finally:
        await engine.dispose()


class TestUpsertEndToEnd:
    async def test_first_run_inserts_all(self, tmp_path):
        """First run with v1 CSV inserts 2 rows."""
        config = _load_config(tmp_path)
        result = await _run(config, "companies_v1.csv")
        assert result.total_inserted == 2

        rows = await _query(config.database.url, "SELECT name, phone_number FROM companies ORDER BY name")
        assert len(rows) == 2
        assert rows[0][0] == "Acme Corp"
        assert rows[0][1] == "(555) 555-1111"

    async def test_second_run_updates_existing_and_inserts_new(self, tmp_path):
        """Second run with v2 CSV updates Acme, leaves Beta alone, adds Gamma."""
        config = _load_config(tmp_path)

        # First run: insert v1
        await _run(config, "companies_v1.csv")

        # Second run: v2 has updated Acme, no Beta, new Gamma
        await _run(config, "companies_v2.csv")

        rows = await _query(
            config.database.url,
            "SELECT name, phone_number FROM companies ORDER BY name"
        )

        # Expect: Acme updated, Beta untouched, Gamma added -> 3 rows
        assert len(rows) == 3

        names = {r[0]: r[1] for r in rows}
        assert names["Acme Corp"] == "(555) 555-9999"  # updated
        assert names["Beta Inc"] == "(555) 555-2222"   # unchanged
        assert names["Gamma LLC"] == "(555) 555-3333"  # new
