"""Rich CLI renderer for human-in-the-loop review of extracted records."""

from __future__ import annotations

import logging

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.table import Table
from rich.tree import Tree

from siphon.core.reviewer import ReviewBatch, ReviewStatus

logger = logging.getLogger("siphon")


class ReviewCLI:
    """Interactive Rich CLI renderer for reviewing a :class:`ReviewBatch`."""

    def __init__(self, console: Console | None = None) -> None:
        self._console = console or Console()

    # ------------------------------------------------------------------
    # Display helpers
    # ------------------------------------------------------------------

    def _display_batch(self, batch: ReviewBatch) -> None:
        """Display the batch for review using Rich components."""
        summary = batch.get_summary()

        # Summary panel
        self._console.print(
            Panel(
                f"[bold]{summary['record_count']}[/bold] records across "
                f"[bold]{len(summary['tables_affected'])}[/bold] tables: "
                f"{', '.join(summary['tables_affected'])}\n"
                f"Status: [bold]{summary['status']}[/bold]",
                title="Review Batch",
            )
        )

        # Records table
        if batch.records:
            table = Table(title="Records Preview", show_lines=True)
            # Use keys from first record as columns
            columns = list(batch.records[0].keys())
            for col in columns:
                table.add_column(col, style="cyan")

            # Show up to 10 records
            for record in batch.records[:10]:
                table.add_row(*[str(record.get(col, "")) for col in columns])

            if len(batch.records) > 10:
                table.add_row(*["..." for _ in columns])

            self._console.print(table)

        # SQL preview
        sql_statements = batch.get_sql_preview()
        if sql_statements:
            tree = Tree("[bold]SQL Preview[/bold]")
            for stmt in sql_statements[:5]:
                tree.add(f"[dim]{stmt}[/dim]")
            if len(sql_statements) > 5:
                tree.add(f"[dim]... and {len(sql_statements) - 5} more[/dim]")
            self._console.print(tree)

    # ------------------------------------------------------------------
    # Interactive review loop
    # ------------------------------------------------------------------

    async def run_review(self, batch: ReviewBatch) -> ReviewBatch:
        """Run the interactive review loop.

        Displays the batch and prompts for action:
        - ``"approve"`` or ``"a"``: approve the batch.
        - ``"reject"`` or ``"r"``: reject the batch.

        Returns the final :class:`ReviewBatch` (approved or rejected).
        """
        while batch.status == ReviewStatus.PENDING:
            self._display_batch(batch)

            self._console.print()
            self._console.print(
                "[bold]Actions:[/bold] [green]approve[/green] (a) | "
                "[red]reject[/red] (r)"
            )

            action = Prompt.ask("Review")
            action_lower = action.strip().lower()

            if action_lower in ("approve", "a"):
                batch.approve()
                self._console.print("[green]\u2713 Batch approved[/green]")
            elif action_lower in ("reject", "r"):
                batch.reject()
                self._console.print("[red]\u2717 Batch rejected[/red]")
            else:
                self._console.print("[yellow]Unknown action. Type 'approve' or 'reject'.[/yellow]")

        return batch
