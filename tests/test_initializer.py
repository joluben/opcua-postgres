"""Tests de la verificación de permisos de aprovisionamiento (PG15+)."""

from pathlib import Path
from types import SimpleNamespace

import pytest

import asyncpg

from connector.db.initializer import (
    InitializationError,
    _create_data_table,
    _ensure_create_privilege,
    initialize_schema,
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
    def __init__(self, *, fail_unique: bool = False, index_exists: bool = False) -> None:
        self.queries = []
        self.fail_unique = fail_unique
        self.index_exists = index_exists

    async def fetchval(self, query, *args):
        if "to_regclass" in query:
            return self.index_exists
        raise AssertionError(f"consulta inesperada: {query}")

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
async def test_unique_index_is_not_recreated_when_it_already_exists():
    """Regresión P0: crash-loop al reiniciar contra una BD ya inicializada.

    TimescaleDB rechaza `CREATE UNIQUE INDEX` sobre una hypertable comprimida
    **aunque el índice ya exista y se use IF NOT EXISTS**; en el segundo arranque
    el índice está creado y la compresión activa, así que el DDL no debe emitirse.
    """
    conn = _SchemaConn(index_exists=True)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", use_timescale=False,
        dedup_enabled=True,
    )

    assert not any("CREATE UNIQUE INDEX" in q for q in conn.queries), (
        "se reemitió el CREATE UNIQUE INDEX con el índice ya existente"
    )


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
    assert "GRANT TEMP ON DATABASE scada_db TO connector_user;" in sql


class _AcquireCtx:
    def __init__(self, conn) -> None:
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, *_exc):
        return False


class _DeadlockOnceConn:
    """Primera sentencia DDL -> deadlock; el resto, correcto."""

    def __init__(self) -> None:
        self.executed = 0
        self.failed = False

    async def fetchval(self, query, *args):
        if "pg_extension" in query:
            return 1
        if "to_regclass" in query:
            return True
        if "has_table_privilege" in query:
            return True
        raise AssertionError(f"consulta inesperada: {query}")

    async def execute(self, query, *args):
        self.executed += 1
        if not self.failed:
            self.failed = True
            raise asyncpg.DeadlockDetectedError("deadlock detected")
        return "OK"


class _DeadlockPool:
    def __init__(self, conn) -> None:
        self.conn = conn
        self.acquires = 0

    def acquire(self):
        self.acquires += 1
        return _AcquireCtx(self.conn)


@pytest.mark.asyncio
async def test_initialize_schema_retries_on_deadlock():
    """DDL + job de compresión de TimescaleDB pueden chocar (deadlock transitorio).

    Sin reintento, el arranque de dos conectores a la vez dependía del ciclo de
    reintento de sesión completo (~30 s con backoff).
    """
    conn = _DeadlockOnceConn()
    pool = _DeadlockPool(conn)
    cfg = SimpleNamespace(
        use_timescale=True, catalog_table="opc_tags_catalog",
        data_table="opc_raw_values", dedup_enabled=False,
    )

    await initialize_schema(pool, cfg)  # no debe propagar el deadlock

    assert pool.acquires == 2, "no se reintentó tras el deadlock"
    assert conn.executed > 1
