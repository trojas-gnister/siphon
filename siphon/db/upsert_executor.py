"""UpsertExecutor — encapsulates the upsert path with diff computation.

Used by the Inserter when on_conflict is configured on a table. Returns
(pk_value, action, field_changes) so the inserter can emit accurate audit
entries.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import insert as sa_insert, select, update as sa_update

from siphon.db.upsert import (
    GenericUpsertPlan,
    build_upsert_statement,
)


class UpsertExecutor:
    """Executes upsert statements and computes field-level diffs.

    Holds a reference to the dialect string and the field-name → column-name
    map. Stateless across calls — safe to reuse across many records.
    """

    def __init__(
        self,
        *,
        dialect: str,
        field_name_to_column: dict[str, str],
    ) -> None:
        self._dialect = dialect
        self._name_to_column = field_name_to_column

    def _names_to_columns(self, names: list[str]) -> list[str]:
        return [self._name_to_column.get(n, n) for n in names]

    async def execute(
        self,
        session,
        model,
        row_data: dict,
        pk_config,
        on_conflict_cfg,
    ) -> tuple[Any, str | None, dict | None]:
        """Run the upsert and return (pk_value, action, field_changes).

        action is one of:
        - "insert" — no existing row matched
        - "update" — existing row had different values; field_changes populated
        - "skip" — explicit on_conflict.action="skip" with an existing row
        - None — no-op (existing row matched but values were identical)

        field_changes is None unless action == "update".
        """
        db_conflict_key = self._names_to_columns(on_conflict_cfg.key)

        if on_conflict_cfg.update_columns == "all":
            db_update_columns = "all"
        else:
            db_update_columns = self._names_to_columns(on_conflict_cfg.update_columns)

        # Look up existing row BEFORE upsert so we can compute field changes
        existing_row = await self._lookup_existing_row(
            session, model, db_conflict_key, row_data
        )

        stmt = build_upsert_statement(
            dialect=self._dialect,
            table=model.__table__,
            row=row_data,
            conflict_key=db_conflict_key,
            action=on_conflict_cfg.action,
            update_columns=db_update_columns,
        )

        if isinstance(stmt, GenericUpsertPlan):
            pk_value = await self._execute_generic_plan(
                session, model, pk_config, stmt
            )
        else:
            await session.execute(stmt)
            pk_value = await self._lookup_pk_by_conflict_key(
                session, model, pk_config, db_conflict_key, row_data
            )

        # Decide action + field_changes
        if existing_row is None:
            return pk_value, "insert", None

        if on_conflict_cfg.action == "skip":
            return pk_value, "skip", None

        # action == "update": compute the diff
        field_changes = self._compute_changes(
            row_data, existing_row, db_conflict_key, db_update_columns, model
        )

        if not field_changes:
            # Same values — no meaningful update happened; suppress audit entry
            return pk_value, None, None

        return pk_value, "update", field_changes

    async def _lookup_existing_row(
        self, session, model, db_conflict_key: list[str], row_data: dict,
    ) -> dict | None:
        """Return a dict of {column: value} for the existing row, or None."""
        stmt = select(model)
        for col_name in db_conflict_key:
            stmt = stmt.where(getattr(model, col_name) == row_data[col_name])
        result = await session.execute(stmt)
        row = result.scalar_one_or_none()
        if row is None:
            return None
        return {col.name: getattr(row, col.name) for col in model.__table__.columns}

    async def _lookup_pk_by_conflict_key(
        self, session, model, pk_config, db_conflict_key: list[str], row_data: dict,
    ) -> Any:
        """SELECT the PK after an upsert, matching on the conflict key."""
        pk_col = getattr(model, pk_config.column)
        stmt = select(pk_col)
        for col_name in db_conflict_key:
            stmt = stmt.where(getattr(model, col_name) == row_data[col_name])
        result = await session.execute(stmt)
        return result.scalar_one()

    async def _execute_generic_plan(
        self, session, model, pk_config, plan: GenericUpsertPlan,
    ) -> Any:
        """Execute a generic select-then-update plan."""
        pk_col = getattr(model, pk_config.column)
        select_stmt = select(pk_col)
        for col_name in plan.conflict_key:
            select_stmt = select_stmt.where(
                getattr(model, col_name) == plan.row[col_name]
            )
        existing = (await session.execute(select_stmt)).scalar_one_or_none()

        if existing is None:
            await session.execute(sa_insert(plan.table).values(**plan.row))
            return (await session.execute(select_stmt)).scalar_one()

        if plan.action == "skip":
            return existing

        if plan.update_columns == "all":
            update_values = {
                k: v for k, v in plan.row.items()
                if k not in plan.conflict_key
            }
        else:
            update_values = {
                k: plan.row[k] for k in plan.update_columns if k in plan.row
            }

        if update_values:
            update_stmt = sa_update(plan.table).values(**update_values)
            for col_name in plan.conflict_key:
                update_stmt = update_stmt.where(
                    getattr(model, col_name) == plan.row[col_name]
                )
            await session.execute(update_stmt)

        return existing

    def _compute_changes(
        self,
        row_data: dict,
        existing_row: dict,
        key_columns: list[str],
        update_columns,
        model,
    ) -> dict[str, dict]:
        """Compute {column: {old, new}} for columns that differ in an upsert."""
        key_set = set(key_columns)

        if update_columns == "all":
            candidates = [
                col.name for col in model.__table__.columns
                if col.name not in key_set and col.name in row_data
            ]
        else:
            candidates = [c for c in update_columns if c in row_data]

        changes = {}
        for col_name in candidates:
            new_val = row_data.get(col_name)
            old_val = existing_row.get(col_name)
            if new_val != old_val:
                changes[col_name] = {"old": old_val, "new": new_val}
        return changes
