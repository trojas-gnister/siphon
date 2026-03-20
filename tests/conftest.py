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
        "llm": {
            "base_url": "https://api.openai.com/v1",
            "model": "gpt-4o-mini",
            "api_key": "sk-test-key",
        },
        "database": {
            "url": "sqlite+aiosqlite:///test.db",
        },
        "schema": {
            "fields": [
                {
                    "name": "company_name",
                    "type": "string",
                    "required": True,
                },
            ],
            "tables": [
                {
                    "name": "companies",
                    "primary_key": "id",
                    "fields": ["company_name"],
                }
            ],
        },
        "pipeline": {
            "chunk_size": 50,
            "review": False,
            "log_level": "info",
        },
    }
