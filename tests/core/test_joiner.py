"""Tests for the Joiner — merge two record lists by a shared key."""

from __future__ import annotations

from siphon.core.joiner import join_records


class TestLeftJoin:
    def test_one_to_one_merges_all_fields(self):
        left = [{"company_code": "A", "company_name": "Acme"}]
        right = [{"company_code": "A", "address": "123 Main"}]
        result = join_records(left, right, on="company_code", join_type="left")
        assert result == [{
            "company_code": "A",
            "company_name": "Acme",
            "address": "123 Main",
        }]

    def test_unmatched_left_keeps_left_fields_with_none_for_right(self):
        left = [
            {"company_code": "A", "company_name": "Acme"},
            {"company_code": "B", "company_name": "Beta"},
        ]
        right = [{"company_code": "A", "address": "1"}]
        result = join_records(left, right, on="company_code", join_type="left")
        assert len(result) == 2
        beta = next(r for r in result if r["company_code"] == "B")
        # Right fields filled with None for unmatched left rows
        assert beta == {"company_code": "B", "company_name": "Beta", "address": None}

    def test_one_to_many_produces_one_row_per_right_match(self):
        left = [{"company_code": "A", "company_name": "Acme"}]
        right = [
            {"company_code": "A", "address": "1"},
            {"company_code": "A", "address": "2"},
        ]
        result = join_records(left, right, on="company_code", join_type="left")
        assert len(result) == 2
        addresses = sorted(r["address"] for r in result)
        assert addresses == ["1", "2"]
        assert all(r["company_name"] == "Acme" for r in result)

    def test_empty_right_keeps_all_left_with_none_added(self):
        left = [{"company_code": "A", "company_name": "Acme"}]
        right = []
        result = join_records(left, right, on="company_code", join_type="left")
        assert result == [{"company_code": "A", "company_name": "Acme"}]

    def test_empty_left_returns_empty(self):
        left = []
        right = [{"company_code": "A", "address": "1"}]
        result = join_records(left, right, on="company_code", join_type="left")
        assert result == []


class TestInnerJoin:
    def test_only_matched_rows_returned(self):
        left = [
            {"k": "A", "x": 1},
            {"k": "B", "x": 2},
        ]
        right = [{"k": "A", "y": 10}]
        result = join_records(left, right, on="k", join_type="inner")
        assert result == [{"k": "A", "x": 1, "y": 10}]

    def test_no_matches_returns_empty(self):
        left = [{"k": "A", "x": 1}]
        right = [{"k": "B", "y": 10}]
        result = join_records(left, right, on="k", join_type="inner")
        assert result == []


class TestKeyHandling:
    def test_missing_key_in_left_treated_as_none(self):
        """A left row with no value at the join key never matches."""
        left = [{"x": 1}]  # no 'k'
        right = [{"k": "A", "y": 10}]
        result = join_records(left, right, on="k", join_type="left")
        # Left join: left row is preserved, right fields are None
        assert len(result) == 1
        assert result[0]["x"] == 1
        assert result[0].get("y") is None

    def test_unknown_join_type_raises(self):
        import pytest
        with pytest.raises(ValueError, match="join_type"):
            join_records([], [], on="k", join_type="cross")

    def test_left_fields_take_precedence_on_key_overlap(self):
        """If left and right both have the same NON-KEY column, left wins."""
        left = [{"k": "A", "shared": "from_left"}]
        right = [{"k": "A", "shared": "from_right"}]
        result = join_records(left, right, on="k", join_type="left")
        assert result[0]["shared"] == "from_left"
