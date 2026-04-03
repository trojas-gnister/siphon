"""Typer CLI for the Siphon ETL pipeline."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from siphon import __version__
from siphon.config.loader import load_config, validate_config
from siphon.core.pipeline import Pipeline, PipelineResult
from siphon.utils.errors import SiphonError

app = typer.Typer(
    name="siphon",
    help="Configurable LLM-powered ETL pipeline",
    no_args_is_help=True,
)
console = Console()


def version_callback(value: bool) -> None:
    if value:
        console.print(f"siphon {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """Configurable LLM-powered ETL pipeline."""
    pass


@app.command()
def run(
    input_path: str = typer.Argument(..., help="Path to spreadsheet file or directory"),
    config: Path = typer.Option(Path("siphon.yaml"), "--config", "-c", help="Path to YAML config"),
    create_tables: bool = typer.Option(False, "--create-tables", help="Auto-create tables if they don't exist"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Extract + validate only, no DB insertion"),
    no_review: bool = typer.Option(False, "--no-review", help="Skip HITL review, insert directly"),
    chunk_size: Optional[int] = typer.Option(None, "--chunk-size", help="Override chunk size from config"),
    sheet: Optional[str] = typer.Option(None, "--sheet", help="Sheet name or index for multi-sheet Excel"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Set log level to debug"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Set log level to error only"),
) -> None:
    """Execute the full ETL pipeline."""
    try:
        cfg = load_config(config)

        # Apply log level overrides
        if verbose:
            cfg.pipeline.log_level = "debug"
        elif quiet:
            cfg.pipeline.log_level = "error"

        pipeline = Pipeline(cfg)
        result = asyncio.run(
            pipeline.run(
                input_path,
                dry_run=dry_run,
                no_review=no_review,
                create_tables=create_tables,
                chunk_size=chunk_size,
            )
        )

        _print_summary(result)

    except SiphonError as e:
        console.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)
    except Exception as e:
        console.print(f"[red]Unexpected error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def validate(
    config: Path = typer.Option(Path("siphon.yaml"), "--config", "-c", help="Path to YAML config"),
) -> None:
    """Validate a config file without running the pipeline."""
    try:
        warnings = validate_config(config)
        console.print("[green]Config is valid[/green]")
        for warning in warnings:
            console.print(f"[yellow]Warning: {warning}[/yellow]")
    except SiphonError as e:
        console.print(f"[red]Config is invalid:[/red] {e}")
        raise typer.Exit(code=1)


INIT_TEMPLATE = '''# Siphon ETL Pipeline Configuration
# ===================================
# This file configures the Siphon data pipeline.
# Uncomment and modify sections as needed.

name: "my-pipeline"

# LLM Configuration
# -----------------
# Siphon uses any OpenAI-compatible API for data extraction.
# Works with: OpenAI, Ollama, vLLM, LM Studio
llm:
  base_url: "http://localhost:11434/v1"  # Ollama default
  model: "llama3"
  api_key: ""  # Required for OpenAI, optional for local models
  # extraction_hints: |
  #   Optional domain-specific instructions for the LLM.
  #   Example: "If company name is missing, use city name instead."

# Database Configuration
# ----------------------
# Any SQLAlchemy-supported database. Install the appropriate driver.
# Examples:
#   SQLite:      sqlite+aiosqlite:///data.db
#   PostgreSQL:  postgresql+asyncpg://user:pass@localhost/dbname
#   MySQL:       mysql+aiomysql://user:pass@localhost/dbname
database:
  url: "${DATABASE_URL}"  # Environment variable substitution supported

# Schema Definition
# -----------------
schema:
  fields:
    # String field (strips whitespace)
    - name: company_name
      type: string
      required: true
      min_length: 2
      # max_length: 255
      db:
        table: companies
        column: name

    # Phone field (formats as US phone number)
    # - name: phone
    #   type: phone
    #   db:
    #     table: companies
    #     column: phone_number

    # URL field (prepends http:// if missing)
    # - name: website
    #   type: url
    #   db:
    #     table: companies
    #     column: website_url

    # Email field (lowercased)
    # - name: email
    #   type: email
    #   db:
    #     table: companies
    #     column: email

    # Integer field
    # - name: employee_count
    #   type: integer
    #   min: 0
    #   max: 1000000
    #   db:
    #     table: companies
    #     column: employees

    # Number (float) field
    # - name: revenue
    #   type: number
    #   db:
    #     table: companies
    #     column: revenue

    # Currency field (strips $, commas; returns Decimal)
    # - name: annual_revenue
    #   type: currency
    #   db:
    #     table: companies
    #     column: revenue

    # Date field (flexible input, configurable output format)
    # - name: founded_date
    #   type: date
    #   format: "%Y-%m-%d"
    #   db:
    #     table: companies
    #     column: founded

    # Datetime field
    # - name: last_updated
    #   type: datetime
    #   format: "%Y-%m-%dT%H:%M:%S"
    #   db:
    #     table: companies
    #     column: updated_at

    # Enum field (with explicit values)
    # - name: status
    #   type: enum
    #   values: [active, inactive, pending]
    #   case: upper  # upper | lower | preserve
    #   db:
    #     table: companies
    #     column: status

    # Enum field (with preset -- US states via pycountry)
    # - name: state
    #   type: enum
    #   preset: us_states
    #   db:
    #     table: addresses
    #     column: state_code

    # Boolean field (detects yes/no/true/false/1/0)
    # - name: is_active
    #   type: boolean
    #   db:
    #     table: companies
    #     column: active

    # Regex field (validates against pattern)
    # - name: tax_id
    #   type: regex
    #   pattern: "^\\\\d{2}-\\\\d{7}$"
    #   db:
    #     table: companies
    #     column: tax_id

    # Subdivision field (ISO 3166-2 subdivision codes)
    # - name: province
    #   type: subdivision
    #   country_code: CA  # ISO 3166-1 alpha-2 country code
    #   db:
    #     table: addresses
    #     column: province_code

    # Country field (ISO 3166-1 alpha-2 country codes)
    # - name: country
    #   type: country
    #   db:
    #     table: addresses
    #     column: country_code

  tables:
    companies:
      primary_key:
        column: id
        type: auto_increment  # auto_increment | uuid

  # Deduplication (optional)
  # deduplication:
  #   key: [company_name]       # Fields to match on
  #   check_db: true            # Also check existing DB rows
  #   match: case_insensitive   # exact | case_insensitive

# Relationships (optional)
# relationships:
#   # Foreign key relationship
#   - type: belongs_to
#     field: parent_entity      # Schema field containing the reference
#     table: companies          # Table that gets the FK column
#     references: companies     # Target table
#     fk_column: parent_id      # Auto-generated FK column name
#     resolve_by: name          # Match against this column in target table
#
#   # Many-to-many junction table
#   - type: junction
#     link: [companies, addresses]
#     through: company_addresses
#     columns:
#       companies: company_id
#       addresses: address_id

# Pipeline Options
pipeline:
  chunk_size: 25        # Rows per LLM extraction batch
  review: true          # Enable human-in-the-loop review
  log_level: info       # debug | info | warning | error
  # log_dir: ./logs     # Directory for log files
'''


@app.command()
def init() -> None:
    """Generate a starter siphon.yaml config file."""
    target = Path("siphon.yaml")
    if target.exists():
        confirm = typer.confirm(f"{target} already exists. Overwrite?", default=False)
        if not confirm:
            raise typer.Exit()

    target.write_text(INIT_TEMPLATE)
    console.print(f"[green]Created {target}[/green]")


def _print_summary(result: PipelineResult) -> None:
    """Print a Rich table summarising the pipeline result."""
    table = Table(title="Pipeline Summary")
    table.add_column("Metric", style="bold")
    table.add_column("Count", justify="right")

    table.add_row("Extracted", str(result.total_extracted))
    table.add_row("Valid", str(result.total_valid))
    table.add_row("Invalid", str(result.total_invalid))
    table.add_row("Duplicates", str(result.total_duplicates))

    if not result.dry_run:
        table.add_row("Inserted", str(result.total_inserted))
    else:
        table.add_row("Inserted", "skipped (dry run)")

    if result.skipped_chunks:
        table.add_row("Skipped Chunks", str(len(result.skipped_chunks)))

    console.print(table)


if __name__ == "__main__":
    app()
