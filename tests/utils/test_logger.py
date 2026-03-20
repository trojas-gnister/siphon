"""Tests for siphon.utils.logger — setup_logging."""

import logging
import sys
from pathlib import Path

import pytest

from siphon.utils.logger import setup_logging


@pytest.fixture(autouse=True)
def reset_loggers():
    """Remove handlers from siphon / siphon.sql after each test."""
    yield
    for name in ("siphon", "siphon.sql"):
        lgr = logging.getLogger(name)
        lgr.handlers.clear()


# ---------------------------------------------------------------------------
# Return value and basic properties
# ---------------------------------------------------------------------------


class TestSetupLoggingReturnValue:
    def test_returns_logger(self):
        logger = setup_logging()
        assert isinstance(logger, logging.Logger)

    def test_logger_name_is_siphon(self):
        logger = setup_logging()
        assert logger.name == "siphon"


# ---------------------------------------------------------------------------
# Console handler
# ---------------------------------------------------------------------------


class TestConsoleHandler:
    def test_has_stream_handler(self):
        logger = setup_logging()
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        assert len(stream_handlers) >= 1

    def test_stream_handler_targets_stderr(self):
        logger = setup_logging()
        stream_handlers = [
            h for h in logger.handlers
            if type(h) is logging.StreamHandler  # exclude FileHandler subclasses
        ]
        assert any(h.stream is sys.stderr for h in stream_handlers)

    def test_format_contains_levelname(self):
        logger = setup_logging()
        handler = next(
            h for h in logger.handlers if type(h) is logging.StreamHandler
        )
        assert "%(levelname)s" in handler.formatter._fmt

    def test_format_contains_message(self):
        logger = setup_logging()
        handler = next(
            h for h in logger.handlers if type(h) is logging.StreamHandler
        )
        assert "%(message)s" in handler.formatter._fmt


# ---------------------------------------------------------------------------
# Log level conversion
# ---------------------------------------------------------------------------


class TestLogLevel:
    @pytest.mark.parametrize("level_str,expected", [
        ("debug", logging.DEBUG),
        ("info", logging.INFO),
        ("warning", logging.WARNING),
        ("error", logging.ERROR),
        ("DEBUG", logging.DEBUG),
        ("INFO", logging.INFO),
        ("WARNING", logging.WARNING),
        ("ERROR", logging.ERROR),
    ])
    def test_level_set_correctly(self, level_str, expected):
        logger = setup_logging(log_level=level_str)
        assert logger.level == expected

    def test_unknown_level_defaults_to_info(self):
        logger = setup_logging(log_level="verbose")
        assert logger.level == logging.INFO


# ---------------------------------------------------------------------------
# File handlers (log_dir provided)
# ---------------------------------------------------------------------------


class TestFileHandlers:
    def test_main_log_file_created(self, tmp_path):
        setup_logging(log_dir=str(tmp_path))
        # Use the timestamped pattern (8 date digits + underscore + 6 time digits)
        log_files = list(tmp_path.glob("siphon_????????_??????.log"))
        assert len(log_files) == 1

    def test_main_log_filename_pattern(self, tmp_path):
        setup_logging(log_dir=str(tmp_path))
        log_files = list(tmp_path.glob("siphon_????????_??????.log"))
        assert len(log_files) == 1

    def test_sql_log_file_created(self, tmp_path):
        setup_logging(log_dir=str(tmp_path))
        sql_log = tmp_path / "siphon_sql.log"
        assert sql_log.exists()

    def test_sql_logger_named_siphon_sql(self, tmp_path):
        setup_logging(log_dir=str(tmp_path))
        sql_logger = logging.getLogger("siphon.sql")
        assert sql_logger.name == "siphon.sql"

    def test_sql_logger_has_rotating_handler(self, tmp_path):
        from logging.handlers import RotatingFileHandler
        setup_logging(log_dir=str(tmp_path))
        sql_logger = logging.getLogger("siphon.sql")
        rotating = [h for h in sql_logger.handlers if isinstance(h, RotatingFileHandler)]
        assert len(rotating) == 1

    def test_sql_rotating_handler_max_bytes(self, tmp_path):
        from logging.handlers import RotatingFileHandler
        setup_logging(log_dir=str(tmp_path))
        sql_logger = logging.getLogger("siphon.sql")
        handler = next(h for h in sql_logger.handlers if isinstance(h, RotatingFileHandler))
        assert handler.maxBytes == 5 * 1024 * 1024

    def test_sql_rotating_handler_backup_count(self, tmp_path):
        from logging.handlers import RotatingFileHandler
        setup_logging(log_dir=str(tmp_path))
        sql_logger = logging.getLogger("siphon.sql")
        handler = next(h for h in sql_logger.handlers if isinstance(h, RotatingFileHandler))
        assert handler.backupCount == 3

    def test_log_dir_created_if_missing(self, tmp_path):
        new_dir = tmp_path / "nested" / "logs"
        assert not new_dir.exists()
        setup_logging(log_dir=str(new_dir))
        assert new_dir.exists()

    def test_no_file_handlers_without_log_dir(self):
        logger = setup_logging()
        file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
        assert len(file_handlers) == 0


# ---------------------------------------------------------------------------
# Idempotency — calling setup_logging twice doesn't duplicate handlers
# ---------------------------------------------------------------------------


class TestIdempotency:
    def test_no_duplicate_handlers_on_second_call(self):
        setup_logging()
        setup_logging()
        logger = logging.getLogger("siphon")
        # Should still have exactly one console handler
        stream_handlers = [h for h in logger.handlers if type(h) is logging.StreamHandler]
        assert len(stream_handlers) == 1
