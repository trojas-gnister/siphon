"""Dialect-aware upsert statement builder.

Builds INSERT ... ON CONFLICT statements for each supported SQL dialect.
The Inserter calls build_upsert_statement() and doesn't care which dialect
is in use — that detection happens here.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import Table

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
