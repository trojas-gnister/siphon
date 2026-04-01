"""Typer CLI for the Siphon ETL pipeline."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from siphon.config.loader import load_config, validate_config
from siphon.core.pipeline import Pipeline, PipelineResult
from siphon.utils.errors import SiphonError

app = typer.Typer(
    name="siphon",
    help="Configurable LLM-powered ETL pipeline",
    no_args_is_help=True,
)
console = Console()


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


@app.command()
def init() -> None:
    """Generate a starter siphon.yaml config file."""
    target = Path("siphon.yaml")
    if target.exists():
        confirm = typer.confirm(f"{target} already exists. Overwrite?", default=False)
        if not confirm:
            raise typer.Exit()

    target.write_text(
        "# Siphon config placeholder\n"
        "# Run 'siphon init' after Task 21 for the full template\n"
    )
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
