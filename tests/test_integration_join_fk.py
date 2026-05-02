"""Multi-source join + FK resolution end-to-end.

Two CSVs (companies + addresses) joined on company_code. The address
records get inserted with a junction row linking them to the
just-inserted company. This verifies the merged dataset's FK resolution
works across source boundaries.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _write_yaml(tmp_path: Path, db_path: Path,
                companies_csv: Path, addresses_csv: Path) -> Path:
    yaml = f"""
name: join-fk-test
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

relationships:
  - type: junction
    link: [companies, addresses]
    through: company_addresses
    columns:
      companies: company_id
      addresses: address_id

pipeline:
  review: false
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _csv(path: Path, headers: list[str], rows: list[list[str]]) -> Path:
    lines = [",".join(headers)]
    for r in rows:
        lines.append(",".join(r))
    path.write_text("\n".join(lines) + "\n")
    return path


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


class TestMultiSourceJoinFKResolution:
    async def test_junction_table_links_correct_company_to_each_address(self, tmp_path):
        """Companies and addresses joined; each address's junction row must
        point to the correct company id."""
        comp = _csv(tmp_path / "companies.csv",
                    ["Name", "Code"],
                    [["Acme", "A001"], ["Beta", "B001"]])
        addr = _csv(tmp_path / "addresses.csv",
                    ["Address", "Company Code"],
                    [["123 Main", "A001"],
                     ["456 Oak", "A001"],
                     ["789 Pine", "B001"]])
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path, comp, addr)

        config = load_config(config_path)
        await Pipeline(config).run(
            input_path="", no_review=True, create_tables=True,
        )

        # NOTE on behavior: the deduplication key is [company_name], so the
        # second Acme join row (the one carrying "456 Oak") is dropped before
        # insert. Surviving rows: (Acme, 123 Main), (Beta, 789 Pine). The
        # "456 Oak" address row never reaches the DB. This locks in current
        # dedup-drops-row behavior — change deliberately if dedup semantics shift.
        addr_rows = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT id, full_address FROM addresses ORDER BY full_address",
        )
        assert len(addr_rows) == 2

        # Junction rows correctly link the two surviving addresses to their companies
        joined = await _query(
            f"sqlite+aiosqlite:///{db_path}",
            "SELECT a.full_address, c.name "
            "FROM addresses a "
            "JOIN company_addresses ca ON ca.address_id = a.id "
            "JOIN companies c ON c.id = ca.company_id "
            "ORDER BY a.full_address",
        )
        assert joined == [
            ("123 Main", "Acme"),
            ("789 Pine", "Beta"),
        ]
