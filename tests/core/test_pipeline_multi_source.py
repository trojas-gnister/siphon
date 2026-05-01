"""Pipeline tests for multi-source loading + joins."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _csv(path: Path, headers: list[str], rows: list[list[str]]) -> Path:
    lines = [",".join(headers)]
    for r in rows:
        lines.append(",".join(r))
    path.write_text("\n".join(lines) + "\n")
    return path


def _yaml(tmp_path: Path, db_path: Path,
          companies_csv: Path, addresses_csv: Path,
          join_type: str = "left") -> Path:
    yaml = f"""
name: multi-source
sources:
  - name: companies
    type: spreadsheet
    path: "{companies_csv}"
    fields:
      - name: company_name
        source: "Name"
        type: string
        required: true
        db: {{ table: companies, column: name }}
      - name: company_code
        source: "Code"
        type: string

  - name: addresses
    type: spreadsheet
    path: "{addresses_csv}"
    fields:
      - name: address
        source: "Address"
        type: string
        db: {{ table: addresses, column: full_address }}
      - name: company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    "on": company_code
    type: {join_type}

database: {{ url: "sqlite+aiosqlite:///{db_path}" }}

schema:
  tables:
    companies: {{ primary_key: {{ column: id, type: auto_increment }} }}
    addresses: {{ primary_key: {{ column: id, type: auto_increment }} }}

pipeline:
  review: false
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


async def _query(db_url: str, sql: str):
    engine = create_async_engine(db_url)
    try:
        async with engine.begin() as conn:
            result = await conn.execute(text(sql))
            if result.returns_rows:
                return result.fetchall()
            return []
    finally:
        await engine.dispose()


class TestMultiSourceLoad:
    async def test_left_join_loads_both_sources_and_inserts(self, tmp_path):
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"], ["Beta", "B001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["123 Main", "A001"]])  # No address for Beta
        db_path = tmp_path / "test.db"
        config_path = _yaml(tmp_path, db_path, comp, addr, join_type="left")

        config = load_config(config_path)
        # Multi-source: input_path is unused; pass empty string
        result = await Pipeline(config).run(
            input_path="",
            no_review=True,
            create_tables=True,
        )

        assert result.total_extracted == 2  # 2 companies post-join
        assert result.total_inserted >= 1

        rows = await _query(f"sqlite+aiosqlite:///{db_path}",
                            "SELECT name FROM companies ORDER BY name")
        names = [r[0] for r in rows]
        assert "Acme" in names
        assert "Beta" in names

        addr_rows = await _query(f"sqlite+aiosqlite:///{db_path}",
                                 "SELECT full_address FROM addresses")
        addresses = [r[0] for r in addr_rows]
        assert "123 Main" in addresses

    async def test_inner_join_drops_unmatched_left_rows(self, tmp_path):
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"], ["Beta", "B001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["123 Main", "A001"]])  # No Beta address
        db_path = tmp_path / "test.db"
        config_path = _yaml(tmp_path, db_path, comp, addr, join_type="inner")

        config = load_config(config_path)
        result = await Pipeline(config).run(
            input_path="",
            no_review=True, create_tables=True,
        )

        # Inner join: only Acme survives
        assert result.total_extracted == 1

    async def test_one_to_many_produces_multiple_addresses(self, tmp_path):
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["1", "A001"], ["2", "A001"], ["3", "A001"]])
        db_path = tmp_path / "test.db"
        config_path = _yaml(tmp_path, db_path, comp, addr, join_type="left")

        config = load_config(config_path)
        result = await Pipeline(config).run(
            input_path="", no_review=True, create_tables=True,
        )

        assert result.total_extracted == 3  # 3 merged rows (one per address)

        addr_rows = await _query(f"sqlite+aiosqlite:///{db_path}",
                                 "SELECT full_address FROM addresses ORDER BY full_address")
        assert [r[0] for r in addr_rows] == ["1", "2", "3"]
