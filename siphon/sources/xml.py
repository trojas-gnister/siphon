from __future__ import annotations

import logging
from pathlib import Path

import xmltodict

from siphon.sources.base import SourceLoader
from siphon.utils.errors import SourceError

logger = logging.getLogger("siphon")


class XMLLoader:
    """Loads XML files into a list of record dicts.

    Navigates to a configurable root path within the XML structure
    and returns the records found there. Nested collections are
    preserved as lists of dicts.
    """

    def __init__(
        self,
        root: str,
        encoding: str = "utf-8",
        force_list: list[str] | None = None,
    ):
        """
        Args:
            root: Dot-separated path to the record list in the XML structure.
                  E.g., "Cases.Case" navigates to data["Cases"]["Case"].
            encoding: File encoding ("utf-8" or "utf-16-le").
            force_list: Element names that should always be parsed as lists,
                       even when only one item exists.
        """
        self._root = root
        self._encoding = encoding
        self._force_list = tuple(force_list) if force_list else ()

    def load(self, path: str | Path, **kwargs) -> list[dict]:
        """Load an XML file and return records at the configured root path.

        Raises SourceError for missing files, parse errors, or invalid paths.
        """
        path = Path(path)
        if not path.exists():
            raise SourceError(f"File not found: {path}")

        try:
            xml_string = path.read_text(encoding=self._encoding)
        except Exception as e:
            raise SourceError(f"Failed to read {path}: {e}") from e

        # Handle duplicate root elements (take first block)
        xml_string = self._deduplicate_root(xml_string)

        try:
            parsed = xmltodict.parse(
                xml_string,
                force_list=self._force_list if self._force_list else None,
            )
        except Exception as e:
            raise SourceError(f"Failed to parse XML: {e}") from e

        # Navigate to root path
        records = self._navigate_path(parsed, self._root)

        if records is None:
            raise SourceError(
                f"Root path '{self._root}' not found in XML structure"
            )

        # Ensure records is a list
        if not isinstance(records, list):
            records = [records]

        logger.info("Loaded %d records from %s", len(records), path.name)
        return records

    def _navigate_path(self, data: dict, path: str):
        """Navigate a dot-separated path in a nested dict."""
        current = data
        for part in path.split("."):
            if isinstance(current, dict):
                current = current.get(part)
            else:
                return None
            if current is None:
                return None
        return current

    def _deduplicate_root(self, xml_string: str) -> str:
        """Handle XML with multiple concatenated root elements.

        Some exports contain duplicated root blocks. We take only the first.
        Detects by finding the root element name and checking for duplicates.
        """
        # Extract root element name from the path
        root_element = self._root.split(".")[0]
        open_tag = f"<{root_element}>"
        close_tag = f"</{root_element}>"

        if xml_string.count(open_tag) > 1:
            # Take first block only
            end_idx = xml_string.find(close_tag)
            if end_idx != -1:
                xml_string = xml_string[: end_idx + len(close_tag)]

        return xml_string
