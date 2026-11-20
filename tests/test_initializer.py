"""Tests de la verificación de permisos de aprovisionamiento (PG15+)."""

from pathlib import Path
from types import SimpleNamespace

import pytest

import asyncpg

from connector.db.initializer import (
    InitializationError,
    _create_data_table,
    _ensure_create_privilege,
    _ensure_schema,
    initialize_schema,
)


class FakeConn:
    def __init__(
        self,
        *,
        table_exists: bool,
        can_create: bool,
        schema_exists: bool = True,
    ) -> None:
        self.table_exists = table_exists
        self.can_create = can_create
        self.schema_exists = schema_exists
        self.checked_create = False
        self.executed: list[str] = []

    async def fetchval(self, query, *args):
        if "to_regnamespace" in query:
            return self.schema_exists
        if "to_regclass" in query:
            return self.table_exists
        if "has_schema_privilege" in query:
            self.checked_create = True
            return self.can_create
        raise AssertionError(f"consulta inesperada: {query}")

    async def execute(self, query, *args):
        self.executed.append(query)
        return "OK"


@pytest.mark.asyncio
async def test_missing_table_without_create_raises_actionable_error():
    conn = FakeConn(table_exists=False, can_create=False)
    with pytest.raises(InitializationError, match='GRANT USAGE, CREATE ON SCHEMA "opcua"'):
        await _ensure_create_privilege(
            conn, "opcua", "opc_tags_catalog", "opc_raw_values"
        )
    assert conn.checked_create is True


@pytest.mark.asyncio
async def test_missing_table_with_create_is_allowed():
    conn = FakeConn(table_exists=False, can_create=True)
    await _ensure_create_privilege(conn, "opcua", "opc_raw_values")


@pytest.mark.asyncio
async def test_existing_table_does_not_require_create():
    conn = FakeConn(table_exists=True, can_create=False)
    await _ensure_create_privilege(conn, "opcua", "opc_raw_values")
    assert conn.checked_create is False


@pytest.mark.asyncio
async def test_existing_schema_is_not_created_again():
    conn = FakeConn(table_exists=True, can_create=True, schema_exists=True)
    await _ensure_schema(conn, "opcua")
    assert conn.executed == []


@pytest.mark.asyncio
async def test_missing_schema_fails_closed_without_creating_it():
    """El conector no crea schemas: sin el schema del DBA no debe tocar la BD."""
    conn = FakeConn(
        table_exists=False, can_create=True, schema_exists=False,
    )
    with pytest.raises(InitializationError, match="no existe") as excinfo:
        await _ensure_schema(conn, "opcua")

    assert "CREATE SCHEMA IF NOT EXISTS" in str(excinfo.value)
    assert conn.executed == [], "el conector no debe intentar crear el schema"


class _SchemaConn:
    def __init__(self, *, fail_unique: bool = False, index_exists: bool = False) -> None:
        self.queries = []
        self.fetch_args = []
        self.fail_unique = fail_unique
        self.index_exists = index_exists

    async def fetchval(self, query, *args):
        if "to_regclass" in query:
            self.fetch_args.append(args)
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
        conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=False,
        dedup_enabled=True,
    )
    ddl = "\n".join(conn.queries)
    assert "event_id      UUID" in ddl
    assert "ADD COLUMN IF NOT EXISTS event_id UUID" in ddl
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "uq_opc_raw_values_event_ts"' in ddl
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
        conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=False,
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
            conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=False,
            dedup_enabled=True,
        )


@pytest.mark.asyncio
async def test_unique_index_not_created_when_dedup_disabled():
    conn = _SchemaConn(fail_unique=True)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=False,
        dedup_enabled=False,
    )
    assert not any("CREATE UNIQUE INDEX" in q for q in conn.queries)


@pytest.mark.asyncio
async def test_data_table_is_qualified_with_configured_schema():
    """Ningún objeto se crea en el schema por defecto: todo va a POSTGRES_SCHEMA."""
    conn = _SchemaConn(index_exists=False)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", use_timescale=True,
        schema="opcua", dedup_enabled=True,
        chunk_interval="7 days", compression_interval="1 month",
        retention_interval="365 days",
    )

    ddl = "\n".join(conn.queries)
    assert 'CREATE TABLE IF NOT EXISTS "opcua"."opc_raw_values"' in ddl
    assert 'REFERENCES "opcua"."opc_tags_catalog"(id)' in ddl
    assert 'create_hypertable(\n                    \'"opcua"."opc_raw_values"\'' in ddl
    assert 'ON "opcua"."opc_raw_values"' in ddl
    assert 'ALTER TABLE "opcua"."opc_raw_values"' in ddl


@pytest.mark.asyncio
async def test_mixed_case_table_indexes_are_quoted_consistently():
    """El nombre del índice debe citarse igual al crearlo y al comprobarlo.

    PostgreSQL pliega a minúsculas los identificadores sin citar. Si se crea
    ``uq_MyData_event_ts`` sin citar pero se busca ``"uq_MyData_event_ts"``, la
    comprobación nunca lo encuentra y en el segundo arranque se intenta recrear
    sobre una hypertable comprimida (crash-loop).
    """
    conn = _SchemaConn(index_exists=False)
    await _create_data_table(
        conn, "MyData", "MyCatalog", schema="opcua", use_timescale=False,
        dedup_enabled=True,
    )

    ddl = "\n".join(conn.queries)
    assert 'CREATE INDEX IF NOT EXISTS "idx_MyData_tag_ts"' in ddl
    assert 'CREATE INDEX IF NOT EXISTS "idx_MyData_quality"' in ddl
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "uq_MyData_event_ts"' in ddl
    assert conn.fetch_args == [('"opcua"."uq_MyData_event_ts"',)]


def test_dba_setup_provisions_configurable_schema():
    sql = (Path(__file__).resolve().parents[1] / "scripts" / "dba_setup.sql").read_text(
        encoding="utf-8"
    )
    assert '\\set schema_name' in sql
    assert 'CREATE SCHEMA IF NOT EXISTS :"schema_name" AUTHORIZATION connector_user;' in sql
    assert 'GRANT USAGE, CREATE ON SCHEMA :"schema_name" TO connector_user;' in sql
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
        if "to_regnamespace" in query:
            return True
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
        use_timescale=True, schema="opcua", catalog_table="opc_tags_catalog",
        data_table="opc_raw_values", dedup_enabled=False,
    )

    await initialize_schema(pool, cfg)  # no debe propagar el deadlock

    assert pool.acquires == 2, "no se reintentó tras el deadlock"
    assert conn.executed > 1


@pytest.mark.asyncio
async def test_timescale_intervals_are_applied_from_configuration():
    conn = _SchemaConn(index_exists=True)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=True,
        dedup_enabled=True,
        chunk_interval="7 days",
        compression_interval="14 days",
        retention_interval="730 days",
    )

    ddl = "\n".join(conn.queries)
    assert "chunk_time_interval => INTERVAL '7 days'" in ddl
    assert (
        """add_compression_policy('"public"."opc_raw_values"', INTERVAL '14 days'"""
        in ddl
    )
    assert (
        """add_retention_policy('"public"."opc_raw_values"', INTERVAL '730 days'"""
        in ddl
    )


@pytest.mark.asyncio
async def test_disabled_policies_are_left_to_the_dba():
    """Con las variables a 'disabled' el conector no impone ninguna política."""
    conn = _SchemaConn(index_exists=True)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=True,
        dedup_enabled=True,
        chunk_interval=None,
        compression_interval=None,
        retention_interval=None,
    )

    ddl = "\n".join(conn.queries)
    assert "create_hypertable" in ddl
    assert "chunk_time_interval" not in ddl
    assert "timescaledb.compress" not in ddl
    assert "add_compression_policy" not in ddl
    assert "add_retention_policy" not in ddl


@pytest.mark.asyncio
async def test_retention_is_applied_even_if_compression_fails():
    class _CompressionFailsConn(_SchemaConn):
        async def execute(self, query, *args):
            self.queries.append(query)
            if "add_compression_policy" in query:
                raise asyncpg.PostgresError("compression policy not supported")
            return "OK"

    conn = _CompressionFailsConn(index_exists=True)
    await _create_data_table(
        conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=True,
        dedup_enabled=True,
        compression_interval="1 month",
        retention_interval="365 days",
    )

    ddl = "\n".join(conn.queries)
    assert "add_compression_policy" in ddl
    assert (
        """add_retention_policy('"public"."opc_raw_values"', INTERVAL '365 days'"""
        in ddl
    )


@pytest.mark.asyncio
async def test_invalid_interval_is_rejected_before_sql_interpolation():
    conn = _SchemaConn(index_exists=True)
    with pytest.raises(InitializationError, match="retention_interval"):
        await _create_data_table(
            conn, "opc_raw_values", "opc_tags_catalog", schema="public", use_timescale=True,
            dedup_enabled=True,
            retention_interval="365 days'; DROP TABLE opc_raw_values;--",
        )
