"""Tests for batched insertion with progress callback."""

from __future__ import annotations

import pytest
from sqlalchemy import select

from siphon.config.schema import SiphonConfig
from siphon.db.engine import DatabaseEngine
from siphon.db.inserter import Inserter
from siphon.db.models import ModelGenerator


def _make_config() -> SiphonConfig:
    return SiphonConfig.model_validate({
        "name": "batched-test",
        "source": {"type": "spreadsheet"},
        "database": {"url": "sqlite+aiosqlite://"},
        "schema": {
            "fields": [
                {"name": "name", "source": "Name", "type": "string",
                 "required": True, "db": {"table": "items", "column": "name"}},
            ],
            "tables": {"items": {"primary_key": {"column": "id", "type": "auto_increment"}}},
        },
        "pipeline": {"review": False},
    })


@pytest.fixture
async def setup():
    config = _make_config()
    engine = DatabaseEngine(config.database)
    model_gen = ModelGenerator(config)
    model_gen.generate()
    await engine.create_tables(model_gen.base)
    yield config, engine, model_gen
    await engine.dispose()


class TestBatchedInsert:
    async def test_batch_size_none_inserts_all_at_once(self, setup):
        """Default batch_size=None preserves v2 behavior."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []

        def on_batch(committed_count: int) -> None:
            progress_calls.append(committed_count)

        records = [{"name": f"r{i}"} for i in range(5)]
        n = await inserter.insert(records, batch_size=None, on_batch=on_batch)

        assert n == 5
        # Single batch -> one progress callback for the whole insert
        assert progress_calls == [5]

    async def test_batch_size_2_invokes_callback_per_batch(self, setup):
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []

        def on_batch(committed_count: int) -> None:
            progress_calls.append(committed_count)

        records = [{"name": f"r{i}"} for i in range(5)]
        n = await inserter.insert(records, batch_size=2, on_batch=on_batch)

        assert n == 5
        # 5 records, batch_size=2 -> batches of [2, 2, 1] -> cumulative [2, 4, 5]
        assert progress_calls == [2, 4, 5]

    async def test_batch_size_larger_than_records_one_batch(self, setup):
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []
        records = [{"name": f"r{i}"} for i in range(3)]
        await inserter.insert(records, batch_size=100,
                              on_batch=lambda n: progress_calls.append(n))

        assert progress_calls == [3]

    async def test_no_callback_does_not_error(self, setup):
        """on_batch is optional."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)
        n = await inserter.insert([{"name": "x"}, {"name": "y"}], batch_size=1)
        assert n == 2

    async def test_async_callback_is_awaited(self, setup):
        """If on_batch is async, the inserter awaits it."""
        config, engine, model_gen = setup
        inserter = Inserter(config, engine, model_gen)

        progress_calls = []

        async def on_batch(committed_count: int) -> None:
            progress_calls.append(committed_count)

        records = [{"name": f"r{i}"} for i in range(4)]
        await inserter.insert(records, batch_size=2, on_batch=on_batch)

        assert progress_calls == [2, 4]

    async def test_batch_failure_rolls_back_only_failing_batch(self, setup):
        """Earlier successfully-committed batches are NOT rolled back."""
        from sqlalchemy import text
        from siphon.utils.errors import DatabaseError

        config, engine, model_gen = setup
        # Add a UNIQUE index so a duplicate fails the second batch.
        # (SQLAlchemy's create_all skips existing tables, so we add a unique
        # index directly via DDL instead of recreating with a UniqueConstraint.)
        async with engine.session() as session:
            await session.execute(
                text("CREATE UNIQUE INDEX uq_items_name ON items (name)")
            )
            await session.commit()

        inserter = Inserter(config, engine, model_gen)

        # First batch: r0, r1 (succeed). Second batch: r1 (duplicate!), r2 (won't reach).
        records = [
            {"name": "r0"},
            {"name": "r1"},
            {"name": "r1"},   # duplicate inside the second batch
            {"name": "r2"},
        ]

        progress_calls = []
        with pytest.raises(DatabaseError):
            await inserter.insert(records, batch_size=2,
                                  on_batch=lambda n: progress_calls.append(n))

        # First batch committed successfully; second batch rolled back.
        async with engine.session() as session:
            existing = await session.execute(select(model_gen.models["items"].name))
            names = sorted([n[0] for n in existing])
        assert names == ["r0", "r1"]
        # Progress was reported once (after the first successful batch)
        assert progress_calls == [2]
