"""Join two record lists by a shared key.

The Joiner is a pure function over flat dict lists — no DB, no I/O.
Used by the pipeline after each source's records have been mapped.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any


def join_records(
    left: list[dict],
    right: list[dict],
    *,
    on: str,
    join_type: str = "left",
) -> list[dict]:
    """Merge two record lists by a shared key.

    Args:
        left: List of left-side record dicts.
        right: List of right-side record dicts.
        on: The key name present in both sides' records.
        join_type: "left" (keep all left rows) or "inner" (only matched rows).

    Returns:
        Merged list of dicts. Each merged dict has all left fields plus all
        right fields. If both sides have a non-key column with the same name,
        the LEFT side's value wins.
    """
    if join_type not in ("left", "inner"):
        raise ValueError(
            f"Unknown join_type: '{join_type}'. Must be 'left' or 'inner'."
        )

    # Index right side by key
    right_index: dict[Any, list[dict]] = defaultdict(list)
    for r in right:
        key_val = r.get(on)
        if key_val is not None:
            right_index[key_val].append(r)

    merged: list[dict] = []

    for left_row in left:
        key_val = left_row.get(on)
        right_matches = right_index.get(key_val) if key_val is not None else None

        if not right_matches:
            if join_type == "inner":
                continue
            # Left join: keep left row, fill right fields with None
            merged.append(_merge_with_nones(left_row, right))
        else:
            for r in right_matches:
                merged.append(_merge(left_row, r))

    return merged


def _merge(left_row: dict, right_row: dict) -> dict:
    """Merge two dicts; left fields take precedence on overlap."""
    return {**right_row, **left_row}


def _merge_with_nones(left_row: dict, right_sample: list[dict]) -> dict:
    """Build a left-join row when no right match exists.

    Right fields are filled with None based on the columns observed in the
    right-side sample.
    """
    if not right_sample:
        return dict(left_row)
    right_columns: set[str] = set()
    for r in right_sample:
        right_columns.update(r.keys())
    null_right = {col: None for col in right_columns if col not in left_row}
    return {**left_row, **null_right}
