"""Logging setup for the Siphon ETL pipeline.

Provides a dual-logger setup:
  - "siphon"     : console (stderr) + optional timestamped file handler
  - "siphon.sql" : optional rotating file handler for SQL statements (5 MB, 3 backups)
"""

import logging
import sys
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path

_LOG_FORMAT = "[%(asctime)s] %(levelname)s - %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

_LEVEL_MAP: dict[str, int] = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}

_SQL_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
_SQL_BACKUP_COUNT = 3


def setup_logging(
    log_level: str = "info",
    log_dir: str | None = None,
) -> logging.Logger:
    """Configure and return the root "siphon" logger.

    Parameters
    ----------
    log_level:
        Severity threshold for the main logger. One of "debug", "info",
        "warning", or "error" (case-insensitive). Defaults to "info".
    log_dir:
        Directory in which to write log files. When *None* only the console
        handler is attached. The directory is created if it does not exist.

    Returns
    -------
    logging.Logger
        The configured "siphon" logger.
    """
    level = _LEVEL_MAP.get(log_level.lower(), logging.INFO)
    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)

    # ------------------------------------------------------------------ #
    # Main "siphon" logger                                                 #
    # ------------------------------------------------------------------ #
    logger = logging.getLogger("siphon")
    logger.setLevel(level)
    # Avoid adding duplicate handlers if called more than once.
    logger.handlers.clear()
    logger.propagate = False

    # Console handler → stderr
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    if log_dir is not None:
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        # Timestamped main log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        main_log_file = log_path / f"siphon_{timestamp}.log"
        file_handler = logging.FileHandler(main_log_file, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # ------------------------------------------------------------------ #
        # SQL sub-logger "siphon.sql"                                         #
        # ------------------------------------------------------------------ #
        sql_logger = logging.getLogger("siphon.sql")
        sql_logger.setLevel(logging.DEBUG)
        sql_logger.handlers.clear()
        sql_logger.propagate = False  # do not bubble up to "siphon"

        sql_log_file = log_path / "siphon_sql.log"
        sql_handler = RotatingFileHandler(
            sql_log_file,
            maxBytes=_SQL_MAX_BYTES,
            backupCount=_SQL_BACKUP_COUNT,
            encoding="utf-8",
        )
        sql_handler.setLevel(logging.DEBUG)
        sql_handler.setFormatter(formatter)
        sql_logger.addHandler(sql_handler)

    return logger
