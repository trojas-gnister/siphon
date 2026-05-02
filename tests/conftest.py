"""Shared pytest fixtures for the Siphon test suite."""

import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_config_dir():
    """Create a temporary directory for config files, yield path, then clean up."""
    tmp_dir = tempfile.mkdtemp(prefix="siphon_test_")
    yield Path(tmp_dir)
    shutil.rmtree(tmp_dir, ignore_errors=True)


@pytest.fixture
def sample_config_dict():
    """Return a minimal valid SiphonConfig as a Python dict with SQLite URL."""
    return {
        "name": "test_pipeline",
        "source": {
            "type": "spreadsheet",
        },
        "database": {
            "url": "sqlite+aiosqlite:///test.db",
        },
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "source": "Company Name",
                    "type": "string",
                    "required": True,
                    "db": {"table": "companies", "column": "name"},
                },
            ],
            "tables": {
                "companies": {
                    "primary_key": {"column": "id", "type": "auto_increment"},
                },
            },
        },
        "pipeline": {
            "review": False,
            "log_level": "info",
        },
    }
