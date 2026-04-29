"""Pipeline integration tests for dry-run diff."""

from __future__ import annotations

from pathlib import Path

import pytest
from siphon.config.schema import SiphonConfig
from siphon.core.pipeline import Pipeline


def _config_yaml(tmp_path: Path, on_conflict_yaml: str = "") -> Path:
    """Write a config file with an optional on_conflict block to tmp_path."""
    db_path = tmp_path / "test.db"
    yaml = f"""
name: diff-test
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
{on_conflict_yaml}
pipeline: {{ review: false }}
"""
    p = tmp_path / "siphon.yaml"
    p.write_text(yaml)
    return p


def _csv(tmp_path: Path, name: str, rows: list[dict]) -> Path:
    """Write a CSV file in tmp_path."""
    p = tmp_path / name
    if not rows:
        p.write_text("Name,Phone\n")
        return p
    headers = list(rows[0].keys())
    lines = [",".join(headers)]
    for row in rows:
        lines.append(",".join(str(row[h]) for h in headers))
    p.write_text("\n".join(lines) + "\n")
    return p


class TestPipelineDryRunDiff:
    async def test_dry_run_without_on_conflict_categorizes_as_insert(self, tmp_path):
        from siphon.config.loader import load_config
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", [
            {"Name": "Acme", "Phone": "111"},
            {"Name": "Beta", "Phone": "222"},
        ])

        config = load_config(config_path)
        pipeline = Pipeline(config)
        result = await pipeline.run(csv_path, dry_run=True, no_review=True,
                                    create_tables=False)

        assert result.dry_run is True
        assert result.diff is not None
        assert len(result.diff["insert"]) == 2
        assert result.diff["update"] == []

    async def test_dry_run_with_on_conflict_categorizes_against_db(self, tmp_path):
        from siphon.config.loader import load_config

        on_conflict_yaml = """      on_conflict:
        key: [name]
        action: update
        update_columns: all"""
        config_path = _config_yaml(tmp_path, on_conflict_yaml)

        # First run: insert two rows
        config = load_config(config_path)
        first_csv = _csv(tmp_path, "first.csv", [
            {"Name": "Acme", "Phone": "OLD"},
            {"Name": "Beta", "Phone": "111"},
        ])
        await Pipeline(config).run(first_csv, no_review=True, create_tables=True)

        # Second run as dry-run: should diff against the DB
        config2 = load_config(config_path)
        second_csv = _csv(tmp_path, "second.csv", [
            {"Name": "Acme", "Phone": "NEW"},      # update
            {"Name": "Beta", "Phone": "111"},      # no_change
            {"Name": "Gamma", "Phone": "333"},     # insert
        ])
        result = await Pipeline(config2).run(
            second_csv, dry_run=True, no_review=True
        )

        assert result.dry_run is True
        assert result.diff is not None
        assert len(result.diff["insert"]) == 1
        assert result.diff["insert"][0]["name"] == "Gamma"
        assert len(result.diff["update"]) == 1
        assert result.diff["update"][0]["record"]["name"] == "Acme"
        assert result.diff["update"][0]["changes"]["phone"]["old"] == "OLD"
        assert result.diff["update"][0]["changes"]["phone"]["new"] == "NEW"
        assert len(result.diff["no_change"]) == 1
        assert result.diff["no_change"][0]["name"] == "Beta"

    async def test_non_dry_run_does_not_compute_diff(self, tmp_path):
        from siphon.config.loader import load_config
        config_path = _config_yaml(tmp_path)
        csv_path = _csv(tmp_path, "data.csv", [{"Name": "Acme", "Phone": "111"}])

        config = load_config(config_path)
        result = await Pipeline(config).run(
            csv_path, dry_run=False, no_review=True, create_tables=True
        )

        assert result.dry_run is False
        assert result.diff is None
