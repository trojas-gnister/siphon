from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from siphon.utils.errors import SourceError

logger = logging.getLogger("siphon")


class SpreadsheetLoader:
    """Loads CSV, XLSX, XLS, and ODS files into a list of record dicts."""

    def load(self, path: str | Path, *, sheet: str | int | None = None) -> list[dict]:
        """Load a spreadsheet and return list of row dicts.

        Args:
            path: Path to the spreadsheet file.
            sheet: Sheet name or 0-based index (for Excel/ODS). Ignored for CSV.

        Returns:
            List of dicts, one per row. Keys are column names from the spreadsheet.

        Raises:
            SourceError: For unsupported formats, missing files, or read errors.
        """
        path = Path(path)
        ext = path.suffix.lower()
        sheet_arg = sheet if sheet is not None else 0

        try:
            if ext == ".csv":
                df = pd.read_csv(path, dtype=str).fillna("")
            elif ext == ".xlsx":
                df = pd.read_excel(
                    path, dtype=str, engine="openpyxl", sheet_name=sheet_arg
                ).fillna("")
            elif ext == ".xls":
                df = pd.read_excel(
                    path, dtype=str, engine="xlrd", sheet_name=sheet_arg
                ).fillna("")
            elif ext == ".ods":
                df = pd.read_excel(
                    path, dtype=str, engine="odf", sheet_name=sheet_arg
                ).fillna("")
            else:
                raise SourceError(f"Unsupported file format: {ext}")
        except SourceError:
            raise
        except Exception as e:
            raise SourceError(f"Failed to read {path}: {e}") from e

        logger.info("Loaded %d rows from %s", len(df), path.name)
        return df.to_dict(orient="records")
