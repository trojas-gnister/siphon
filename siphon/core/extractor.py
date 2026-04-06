"""Spreadsheet extractor: loads a spreadsheet, chunks it, and calls the LLM."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

import pandas as pd

from siphon.config.schema import SiphonConfig
from siphon.llm.client import LLMClient
from siphon.llm.prompts import build_correction_prompt, build_extraction_prompt
from siphon.utils.errors import ExtractionError

logger = logging.getLogger("siphon")


class Extractor:
    """Loads a spreadsheet, splits it into chunks, and extracts records via LLM.

    Supported formats: .csv, .xlsx, .xls, .ods
    """

    def __init__(self, config: SiphonConfig, llm_client: LLMClient) -> None:
        self._config = config
        self._llm = llm_client
        self._skipped_chunks: list[dict] = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def load_spreadsheet(
        self, path: str | Path, *, sheet: str | int | None = None
    ) -> pd.DataFrame:
        """Load a spreadsheet file into a DataFrame.

        Supports: .csv, .xlsx, .xls, .ods

        Parameters
        ----------
        path:
            Path to the spreadsheet file.
        sheet:
            Sheet name or 0-based index for multi-sheet Excel files.
            Ignored for CSV files. Defaults to the first sheet.

        Raises
        ------
        ExtractionError
            For unsupported formats or read errors.
        """
        path = Path(path)
        ext = path.suffix.lower()
        sheet_arg = sheet if sheet is not None else 0
        try:
            if ext == ".csv":
                return pd.read_csv(path, dtype=str).fillna("")
            elif ext == ".xlsx":
                return pd.read_excel(
                    path, dtype=str, engine="openpyxl", sheet_name=sheet_arg
                ).fillna("")
            elif ext == ".xls":
                return pd.read_excel(
                    path, dtype=str, engine="xlrd", sheet_name=sheet_arg
                ).fillna("")
            elif ext == ".ods":
                return pd.read_excel(
                    path, dtype=str, engine="odf", sheet_name=sheet_arg
                ).fillna("")
            else:
                raise ExtractionError(f"Unsupported file format: {ext}")
        except ExtractionError:
            raise
        except Exception as e:
            raise ExtractionError(f"Failed to read {path}: {e}") from e

    def chunk_dataframe(self, df: pd.DataFrame, chunk_size: int) -> list[pd.DataFrame]:
        """Split *df* into a list of DataFrames each with at most *chunk_size* rows."""
        return [df.iloc[i : i + chunk_size] for i in range(0, len(df), chunk_size)]

    async def extract(
        self, path: str | Path, *, sheet: str | int | None = None
    ) -> tuple[list[dict], list[dict]]:
        """Full extraction pipeline: load → chunk → extract via LLM concurrently.

        Parameters
        ----------
        path:
            Path to the spreadsheet file.
        sheet:
            Sheet name or 0-based index for multi-sheet Excel files.

        Returns
        -------
        tuple[list[dict], list[dict]]
            ``(records, skipped_chunks)`` where *records* is the flat list of
            extracted dicts and *skipped_chunks* describes chunks that were
            dropped.
        """
        self._skipped_chunks = []
        df = self.load_spreadsheet(path, sheet=sheet)
        chunk_size = self._config.pipeline.chunk_size
        chunks = self.chunk_dataframe(df, chunk_size)

        logger.info(
            "Loaded %d rows, split into %d chunks of %d",
            len(df),
            len(chunks),
            chunk_size,
        )

        tasks = []
        for i, chunk in enumerate(chunks):
            start_row = i * chunk_size + 1  # 1-based row numbers
            tasks.append(self._extract_chunk(chunk, i, start_row))
        results = await asyncio.gather(*tasks)

        all_records: list[dict] = []
        for chunk_records in results:
            all_records.extend(chunk_records)

        logger.info(
            "Extracted %d records, skipped %d chunks",
            len(all_records),
            len(self._skipped_chunks),
        )
        return all_records, self._skipped_chunks

    @property
    def skipped_chunks(self) -> list[dict]:
        """Most recently skipped chunks (populated after :meth:`extract`)."""
        return self._skipped_chunks

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _extract_chunk(
        self, chunk_df: pd.DataFrame, chunk_index: int, start_row: int
    ) -> list[dict]:
        """Extract data from a single chunk via the LLM.

        Args:
            chunk_df: The chunk DataFrame
            chunk_index: Index of this chunk (0-based)
            start_row: The starting row number in the original spreadsheet (1-based)

        Retry strategy
        --------------
        1. On row-count mismatch: retry once with a correction prompt appended.
        2. If the retry also mismatches (or raises), skip the chunk.
        3. On any :class:`~siphon.utils.errors.ExtractionError` from the LLM:
           skip the chunk immediately.
        """
        chunk_csv = chunk_df.to_csv(index=False)
        row_count = len(chunk_df)
        end_row = start_row + row_count - 1
        row_range = f"rows {start_row}-{end_row}"
        fields = self._config.schema_.fields
        hints = self._config.llm.extraction_hints

        prompt = build_extraction_prompt(fields, chunk_csv, row_count, hints)

        # --- first attempt -------------------------------------------
        try:
            records = await self._llm.extract_json(prompt)
        except ExtractionError as exc:
            logger.warning("Chunk %d: LLM extraction failed: %s", chunk_index, exc)
            self._skipped_chunks.append(
                {
                    "chunk": chunk_index,
                    "row_range": row_range,
                    "rows_affected": row_count,
                    "reason": f"LLM error: {exc}",
                }
            )
            return []

        # --- row-count check -----------------------------------------
        if len(records) != row_count:
            logger.warning(
                "Chunk %d: expected %d records, got %d. Retrying…",
                chunk_index,
                row_count,
                len(records),
            )
            correction = build_correction_prompt(row_count, len(records))
            retry_prompt = prompt + "\n\n" + correction

            try:
                records = await self._llm.extract_json(retry_prompt)
            except ExtractionError as exc:
                logger.warning("Chunk %d: retry failed: %s", chunk_index, exc)
                self._skipped_chunks.append(
                    {
                        "chunk": chunk_index,
                        "row_range": row_range,
                        "rows_affected": row_count,
                        "reason": f"Retry failed: {exc}",
                    }
                )
                return []

            if len(records) != row_count:
                logger.warning(
                    "Chunk %d: still got %d records after retry. Skipping.",
                    chunk_index,
                    len(records),
                )
                self._skipped_chunks.append(
                    {
                        "chunk": chunk_index,
                        "row_range": row_range,
                        "rows_affected": row_count,
                        "reason": (
                            f"Row count mismatch: expected {row_count}, "
                            f"got {len(records)}"
                        ),
                    }
                )
                return []

        return records
