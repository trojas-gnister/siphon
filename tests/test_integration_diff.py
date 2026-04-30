"""End-to-end test for dry-run diff with a real file-based SQLite DB."""

from __future__ import annotations

from pathlib import Path

import pytest
from siphon.config.loader import load_config
from siphon.core.pipeline import Pipeline


def _write_yaml(tmp_path: Path, db_path: Path) -> Path:
    yaml = f"""
name: integration-diff
source: {{ type: spreadsheet }}
database: {{ url: "sqlite+aiosqlite:///{db_path}" }}
schema:
  fields:
    - name: name
      source: "Name"
      type: string
      required: true
      db: {{ table: companies, column: name }}
    - name: phone
      source: "Phone"
      type: string
      db: {{ table: companies, column: phone }}
  tables:
    companies:
      primary_key: {{ column: id, type: auto_increment }}
      on_conflict:
        key: [name]
        action: update
        update_columns: all
pipeline: {{ review: false }}
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _write_csv(tmp_path: Path, name: str, rows: list[dict]) -> Path:
    p = tmp_path / name
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row[h]) for h in headers))
    p.write_text("\n".join(lines) + "\n")
    return p


class TestDryRunDiffEndToEnd:
    async def test_full_diff_flow(self, tmp_path):
        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)

        # First: insert two rows for real
        first_csv = _write_csv(tmp_path, "first.csv", [
            {"Name": "Acme", "Phone": "OLD"},
            {"Name": "Beta", "Phone": "111"},
        ])
        first = await Pipeline(load_config(config_path)).run(
            first_csv, no_review=True, create_tables=True
        )
        assert first.total_inserted == 2

        # Second: dry-run with mixed cases
        second_csv = _write_csv(tmp_path, "second.csv", [
            {"Name": "Acme", "Phone": "NEW"},
            {"Name": "Beta", "Phone": "111"},
            {"Name": "Gamma", "Phone": "333"},
        ])
        second = await Pipeline(load_config(config_path)).run(
            second_csv, dry_run=True, no_review=True
        )

        assert second.dry_run is True
        assert second.total_inserted == 0  # nothing committed
        assert second.diff is not None

        # Insert: Gamma
        assert len(second.diff["insert"]) == 1
        assert second.diff["insert"][0]["name"] == "Gamma"

        # Update: Acme
        assert len(second.diff["update"]) == 1
        u = second.diff["update"][0]
        assert u["key"] == {"name": "Acme"}
        assert u["changes"]["phone"]["old"] == "OLD"
        assert u["changes"]["phone"]["new"] == "NEW"

        # No change: Beta
        assert len(second.diff["no_change"]) == 1
        assert second.diff["no_change"][0]["name"] == "Beta"

    async def test_dry_run_does_not_modify_db(self, tmp_path):
        from sqlalchemy import text
        from sqlalchemy.ext.asyncio import create_async_engine

        db_path = tmp_path / "test.db"
        config_path = _write_yaml(tmp_path, db_path)

        # Set up initial state
        first = _write_csv(tmp_path, "first.csv", [{"Name": "Acme", "Phone": "OLD"}])
        await Pipeline(load_config(config_path)).run(
            first, no_review=True, create_tables=True
        )

        # Dry-run that would update Acme
        second = _write_csv(tmp_path, "second.csv", [{"Name": "Acme", "Phone": "NEW"}])
        await Pipeline(load_config(config_path)).run(
            second, dry_run=True, no_review=True
        )

        # Verify the DB still has OLD
        engine = create_async_engine(f"sqlite+aiosqlite:///{db_path}")
        try:
            async with engine.connect() as conn:
                result = await conn.execute(text("SELECT phone FROM companies"))
                phones = [r[0] for r in result.fetchall()]
        finally:
            await engine.dispose()

        assert phones == ["OLD"]
