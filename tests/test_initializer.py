"""Tests de la verificación de permisos de aprovisionamiento (PG15+)."""

from pathlib import Path

import pytest

import asyncpg

from connector.db.initializer import (
    InitializationError,
    _create_data_table,
    _ensure_create_privilege,
)


class FakeConn:
    def __init__(self, *, table_exists: bool, can_create: bool) -> None:
        self.table_exists = table_exists
        self.can_create = can_create
        self.checked_create = False

    async def fetchval(self, query, *args):
        if "to_regclass" in query:
            return self.table_exists
        if "current_schema" in query:
            return "public"
        if "has_schema_privilege" in query:
            self.checked_create = True
            return self.can_create
        raise AssertionError(f"consulta inesperada: {query}")


@pytest.mark.asyncio
async def test_missing_table_without_create_raises_actionable_error():
    conn = FakeConn(table_exists=False, can_create=False)
    with pytest.raises(InitializationError, match="GRANT USAGE, CREATE ON SCHEMA public"):
        await _ensure_create_privilege(conn, "opc_tags_catalog", "opc_raw_values")
    assert conn.checked_create is True


@pytest.mark.asyncio
async def test_missing_table_with_create_is_allowed():
    conn = FakeConn(table_exists=False, can_create=True)
    await _ensure_create_privilege(conn, "opc_raw_values")


@pytest.mark.asyncio
async def test_existing_table_does_not_require_create():
    conn = FakeConn(table_exists=True, can_create=False)
    await _ensure_create_privilege(conn, "opc_raw_values")
    assert conn.checked_create is False


class _SchemaConn:
    def __init__(self, *, fail_unique: bool = False) -> None:
        self.queries = []
        self.fail_unique = fail_unique

    async def execute(self, query, *args):
        self.queries.append(query)
        if self.fail_unique and "CREATE UNIQUE INDEX" in query:
            raise asyncpg.PostgresError("no unique index on hypertable")
        return "OK"


@pytest.mark.asyncio
async def test_data_table_gets_event_id_and_unique_index():
    conn = _SchemaConn()
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", use_timescale=False,
        dedup_enabled=True,
    )
    ddl = "\n".join(conn.queries)
    assert "event_id      UUID" in ddl
    assert "ADD COLUMN IF NOT EXISTS event_id UUID" in ddl
    assert "CREATE UNIQUE INDEX IF NOT EXISTS uq_opc_raw_values_event_ts" in ddl
    assert "WHERE event_id IS NOT NULL" in ddl


@pytest.mark.asyncio
async def test_missing_unique_index_fails_closed_when_dedup_enabled():
    conn = _SchemaConn(fail_unique=True)
    with pytest.raises(InitializationError, match="deduplicación"):
        await _create_data_table(
            conn, "opc_raw_values", "opc_tags_catalog", use_timescale=False,
            dedup_enabled=True,
        )


@pytest.mark.asyncio
async def test_unique_index_not_created_when_dedup_disabled():
    conn = _SchemaConn(fail_unique=True)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", use_timescale=False,
        dedup_enabled=False,
    )
    assert not any("CREATE UNIQUE INDEX" in q for q in conn.queries)


def test_dba_setup_grants_create_on_schema():
    sql = (Path(__file__).resolve().parents[1] / "scripts" / "dba_setup.sql").read_text(
        encoding="utf-8"
    )
    assert "GRANT USAGE, CREATE ON SCHEMA public TO connector_user;" in sql
