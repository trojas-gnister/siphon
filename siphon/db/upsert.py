"""Dialect-aware upsert statement builder.

Builds INSERT ... ON CONFLICT statements for each supported SQL dialect.
The Inserter calls build_upsert_statement() and doesn't care which dialect
is in use — that detection happens here.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

logger = logging.getLogger("siphon")


def detect_dialect(database_url: str) -> str:
    """Map a SQLAlchemy URL to a dialect name.

    Returns one of: "sqlite", "postgresql", "mysql", "generic".
    """
    url_lower = database_url.lower()
    if url_lower.startswith("sqlite"):
        return "sqlite"
    if url_lower.startswith("postgresql") or url_lower.startswith("postgres"):
        return "postgresql"
    if url_lower.startswith("mysql") or url_lower.startswith("mariadb"):
        return "mysql"
    return "generic"


def build_upsert_statement(
    *,
    dialect: str,
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    """Build an upsert (INSERT ... ON CONFLICT) statement for the given dialect.

    Args:
        dialect: One of "sqlite", "postgresql", "mysql", "generic"
        table: SQLAlchemy Table object
        row: Dict of column name -> value to insert
        conflict_key: List of column names that form the unique conflict target
        action: "update" | "skip" | "error"
        update_columns: "all" or list of column names to update on conflict

    Returns:
        A SQLAlchemy executable Insert statement.
    """
    if dialect == "sqlite":
        return _build_sqlite_upsert(table, row, conflict_key, action, update_columns)
    if dialect == "postgresql":
        return _build_postgres_upsert(table, row, conflict_key, action, update_columns)
    raise NotImplementedError(f"Upsert not yet supported for dialect: {dialect}")


def _build_sqlite_upsert(
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    stmt = sqlite_insert(table).values(**row)

    if action == "error":
        return stmt

    if action == "skip":
        return stmt.on_conflict_do_nothing(index_elements=conflict_key)

    if update_columns == "all":
        update_set = {
            col.name: stmt.excluded[col.name]
            for col in table.columns
            if col.name not in conflict_key and col.name in row
        }
    else:
        update_set = {
            col_name: stmt.excluded[col_name]
            for col_name in update_columns
            if col_name in row
        }

    if not update_set:
        return stmt.on_conflict_do_nothing(index_elements=conflict_key)

    return stmt.on_conflict_do_update(
        index_elements=conflict_key,
        set_=update_set,
    )


def _build_postgres_upsert(
    table: Table,
    row: dict[str, Any],
    conflict_key: list[str],
    action: str,
    update_columns: str | list[str],
):
    stmt = postgres_insert(table).values(**row)

    if action == "error":
        return stmt

    if action == "skip":
        return stmt.on_conflict_do_nothing(index_elements=conflict_key)

    if update_columns == "all":
        update_set = {
            col.name: stmt.excluded[col.name]
            for col in table.columns
            if col.name not in conflict_key and col.name in row
        }
    else:
        update_set = {
            col_name: stmt.excluded[col_name]
            for col_name in update_columns
            if col_name in row
        }

    if not update_set:
        return stmt.on_conflict_do_nothing(index_elements=conflict_key)

    return stmt.on_conflict_do_update(
        index_elements=conflict_key,
        set_=update_set,
    )
