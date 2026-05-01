"""End-to-end test for multi-source pipelines with joins."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


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


def _write_companies(path: Path) -> Path:
    path.write_text(
        "Name,Code\n"
        "Acme,A001\n"
        "Beta,B001\n"
        "Gamma,G001\n"
    )
    return path


def _write_addresses(path: Path) -> Path:
    path.write_text(
        "Address,Company Code,State\n"
        "123 Main,A001,CA\n"
        "456 Oak,A001,CA\n"
        "789 Pine,B001,NY\n"
    )
    return path


def _write_yaml(tmp_path: Path, db_path: Path,
                companies_csv: Path, addresses_csv: Path) -> Path:
    yaml = f"""
name: e2e-multi-source
sources:
  - name: companies
    type: spreadsheet
    path: "{companies_csv}"
    fields:
      - name: company_name
        source: "Name"
        type: string
        required: true
        min_length: 2
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
      - name: state
        source: "State"
        type: enum
        preset: us_states
        db: {{ table: addresses, column: state_code }}
      - name: company_code
        source: "Company Code"
        type: string

joins:
  - left: companies
    right: addresses
    "on": company_code
    type: left

database: {{ url: "sqlite+aiosqlite:///{db_path}" }}

schema:
  tables:
    companies: {{ primary_key: {{ column: id, type: auto_increment }} }}
    addresses: {{ primary_key: {{ column: id, type: auto_increment }} }}
  deduplication:
    key: [company_name]
    check_db: false
    match: case_insensitive

pipeline:
  review: false
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


class TestMultiSourceEndToEnd:
    async def test_two_csvs_join_and_insert_into_multiple_tables(self, tmp_path):
        comp = _write_companies(tmp_path / "companies.csv")
        addr = _write_addresses(tmp_path / "addresses.csv")
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path, comp, addr)

        config = load_config(config_path)
        result = await Pipeline(config).run(
            input_path="",  # ignored for multi-source
            no_review=True,
            create_tables=True,
        )

        # Acme has 2 addresses → 2 merged rows before dedup
        # Beta has 1 address  → 1 merged row
        # Gamma has 0 addresses → 1 merged row (left join, null address fields)
        # Total extracted = 4
        assert result.total_extracted == 4

        # Deduplication key is company_name: both Acme rows share the same key,
        # so the second Acme row (456 Oak) is dropped → 1 duplicate
        assert result.total_duplicates == 1

        # 3 rows survive dedup (first Acme, Beta, Gamma) → 3 inserted
        assert result.total_inserted == 3

        # Companies: Acme, Beta, Gamma each land in the companies table → 3 rows
        company_rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT name FROM companies ORDER BY name",
        )
        assert [r[0] for r in company_rows] == ["Acme", "Beta", "Gamma"]

        # Addresses: first Acme (123 Main, CA) and Beta (789 Pine, NY) inserted;
        # second Acme (456 Oak) was deduped; Gamma had no address (null row filtered).
        addr_rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT full_address, state_code FROM addresses ORDER BY full_address",
        )
        # Filter out any null rows from Gamma's left-join match with no right side
        addresses = [(r[0], r[1]) for r in addr_rows if r[0] is not None]
        assert addresses == [
            ("123 Main", "CA"),
            ("789 Pine", "NY"),
        ]
