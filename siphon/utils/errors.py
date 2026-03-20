"""Custom exception hierarchy for the Siphon ETL pipeline."""


class SiphonError(Exception):
    """Base exception for all Siphon errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message

    def __str__(self) -> str:
        return self.message


class ConfigError(SiphonError):
    """Raised for invalid YAML, missing required fields, or unknown types."""


class ExtractionError(SiphonError):
    """Raised when an LLM call fails, returns an unparseable response, or produces a row count mismatch."""


class ValidationError(SiphonError):
    """Raised when a record fails Pydantic validation."""


class DatabaseError(SiphonError):
    """Raised when a database connection fails, an insert fails, or a table doesn't exist."""


class ReviewError(SiphonError):
    """Raised when a revision fails or an invalid review action is provided."""
