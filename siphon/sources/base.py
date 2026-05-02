from __future__ import annotations

from pathlib import Path
from typing import Protocol, runtime_checkable


@runtime_checkable
class SourceLoader(Protocol):
    """Protocol for source data loaders.

    Every loader must implement load() which returns a flat list of record dicts.
    Keys in each dict are source column/field names exactly as they appear in the source.
    """

    def load(self, path: str | Path, **kwargs) -> list[dict]:
        ...
