"""Tests for siphon.utils.errors — exception hierarchy."""

import pytest

from siphon.utils.errors import (
    ConfigError,
    DatabaseError,
    ExtractionError,
    ReviewError,
    SiphonError,
    ValidationError,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SUBCLASSES = [
    ConfigError,
    ExtractionError,
    ValidationError,
    DatabaseError,
    ReviewError,
]


# ---------------------------------------------------------------------------
# SiphonError base
# ---------------------------------------------------------------------------


class TestSiphonError:
    def test_is_exception_subclass(self):
        assert issubclass(SiphonError, Exception)

    def test_stores_message(self):
        err = SiphonError("base error")
        assert err.message == "base error"

    def test_str_returns_message(self):
        err = SiphonError("base error")
        assert str(err) == "base error"

    def test_can_be_raised_and_caught(self):
        with pytest.raises(SiphonError):
            raise SiphonError("raised")


# ---------------------------------------------------------------------------
# Subclass common behaviour (parametrised)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("exc_class", _SUBCLASSES)
class TestSubclassCommon:
    def test_is_subclass_of_siphon_error(self, exc_class):
        assert issubclass(exc_class, SiphonError)

    def test_is_subclass_of_exception(self, exc_class):
        assert issubclass(exc_class, Exception)

    def test_stores_message(self, exc_class):
        msg = f"{exc_class.__name__} message"
        err = exc_class(msg)
        assert err.message == msg

    def test_str_returns_message(self, exc_class):
        msg = f"{exc_class.__name__} str"
        err = exc_class(msg)
        assert str(err) == msg

    def test_caught_as_siphon_error(self, exc_class):
        msg = "caught as base"
        with pytest.raises(SiphonError) as exc_info:
            raise exc_class(msg)
        assert exc_info.value.message == msg

    def test_caught_as_exception(self, exc_class):
        with pytest.raises(Exception):
            raise exc_class("caught as Exception")

    def test_caught_as_own_type(self, exc_class):
        with pytest.raises(exc_class):
            raise exc_class("own type")


# ---------------------------------------------------------------------------
# Individual subclass identity checks
# ---------------------------------------------------------------------------


class TestConfigError:
    def test_is_config_error(self):
        assert isinstance(ConfigError("bad config"), ConfigError)


class TestExtractionError:
    def test_is_extraction_error(self):
        assert isinstance(ExtractionError("llm failed"), ExtractionError)


class TestValidationError:
    def test_is_validation_error(self):
        assert isinstance(ValidationError("invalid record"), ValidationError)


class TestDatabaseError:
    def test_is_database_error(self):
        assert isinstance(DatabaseError("connection failed"), DatabaseError)


class TestReviewError:
    def test_is_review_error(self):
        assert isinstance(ReviewError("invalid action"), ReviewError)
