"""Aceptación con PostgreSQL/TimescaleDB real y un rol restringido.

Se ejecutan con `scripts/acceptance_pg.sh` (levanta un contenedor efímero) o
contra una BD preproductiva:

    ACCEPTANCE_CONNECTOR_DSN=postgresql://connector_user:...@host/scada_db \
    ACCEPTANCE_ADMIN_DSN=postgresql://postgres:...@host/scada_db \
    pytest tests/integration -q -m integration

El rol `connector_user` debe existir y haberse aprovisionado con
`scripts/dba_setup.sql` (sin privilegios de superusuario).
"""

from __future__ import annotations

import asyncio
import os
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import asyncpg
import pytest
from asyncua import ua

from connector.db.initializer import initialize_schema
from connector.db.spill import SpillBuffer
from connector.db.writer import BatchWriter
from connector.utils.status import to_quality

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("ACCEPTANCE_CONNECTOR_DSN"),
        reason="requiere ACCEPTANCE_CONNECTOR_DSN (ver scripts/acceptance_pg.sh)",
    ),
]

_DSN = os.getenv("ACCEPTANCE_CONNECTOR_DSN", "")
_ADMIN_DSN = os.getenv("ACCEPTANCE_ADMIN_DSN") or _DSN
# El conector ya no escribe en `public`: las pruebas usan un schema propio que
# aprovisiona `scripts/dba_setup.sql` (el conector no crea schemas).
_SCHEMA = os.getenv("ACCEPTANCE_SCHEMA", "opcua")

# El DROP puede chocar con un job de TimescaleDB en vuelo (ver `_drop`).
_DROP_ATTEMPTS = 4
_DROP_RETRY_BASE_S = 0.5


def _names() -> tuple[str, str]:
    token = uuid.uuid4().hex[:12]
    return f"acc_catalog_{token}", f"acc_data_{token}"


def _q(table: str) -> str:
    return f'"{_SCHEMA}"."{table}"'


def _cfg(catalog: str, data: str) -> SimpleNamespace:
    return SimpleNamespace(
        use_timescale=True,
        schema=_SCHEMA,
        catalog_table=catalog,
        data_table=data,
        dedup_enabled=True,
    )


async def _drop(conn: asyncpg.Connection, data: str, catalog: str) -> None:
    """Borra las tablas creadas por el test.

    `add_compression_policy`/`add_retention_policy` registran jobs cuyo
    `next_start` es ~ahora, así que el job puede ejecutarse justo cuando el test
    termina: el `DROP TABLE` espera el advisory lock del job y el job espera el
    lock de la hypertable → PostgreSQL detecta deadlock. Se desregistran primero
    las políticas, se acota `lock_timeout` y se reintenta el DROP (mismo patrón
    que `initialize_schema` aplica a su propio DDL).
    """
    await conn.execute("SET lock_timeout = '10s'")
    try:
        for policy in ("compression", "retention"):
            try:
                await conn.execute(
                    f"SELECT remove_{policy}_policy($1::regclass, if_exists => TRUE)",
                    _q(data),
                )
            except asyncpg.PostgresError:
                # Tabla no-hypertable o Timescale sin cargar: el DROP basta.
                pass
        for table in (data, catalog):
            await _drop_table(conn, table)
    finally:
        await conn.execute("RESET lock_timeout")


async def _drop_table(conn: asyncpg.Connection, table: str) -> None:
    for attempt in range(1, _DROP_ATTEMPTS + 1):
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {_q(table)} CASCADE")
            return
        except (asyncpg.exceptions.DeadlockDetectedError,
                asyncpg.exceptions.LockNotAvailableError):
            if attempt == _DROP_ATTEMPTS:
                raise
            await asyncio.sleep(_DROP_RETRY_BASE_S * attempt)


async def _prepare_schema() -> tuple[str, str, asyncpg.Pool]:
    catalog, data = _names()
    pool = await asyncpg.create_pool(_DSN, min_size=1, max_size=2)
    await initialize_schema(pool, _cfg(catalog, data))
    return catalog, data, pool


async def _tag_id(pool: asyncpg.Pool, catalog: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"INSERT INTO {_q(catalog)} (node_id, display_name) "
            f"VALUES ('ns=2;s=Acceptance', 'Acceptance') RETURNING id"
        )


def _writer_cfg(data: str, *, dedup: bool) -> SimpleNamespace:
    return SimpleNamespace(
        schema=_SCHEMA,
        data_table=data,
        batch_size=100,
        flush_interval_ms=500,
        dedup_enabled=dedup,
    )


def _spill_cfg(tmp_path) -> SimpleNamespace:
    return SimpleNamespace(
        spill_enabled=True,
        spill_dir=str(tmp_path),
        spill_max_mb=10,
        spill_segment_mb=1,
        spill_drain_timeout_s=5,
    )


def _record(tag_id: int, quality: int = 0) -> tuple:
    return (
        uuid.uuid4(), tag_id, datetime.now(timezone.utc), 1.0, None, quality,
        "connector-acceptance",
    )


@pytest.mark.asyncio
async def test_connector_role_is_restricted_and_can_initialize_schema():
    conn = await asyncpg.connect(_DSN)
    try:
        row = await conn.fetchrow(
            "SELECT current_user AS name, usesuper FROM pg_user "
            "WHERE usename = current_user"
        )
        assert row["name"] == "connector_user"
        assert row["usesuper"] is False
    finally:
        await conn.close()

    catalog, data = _names()
    pool = await asyncpg.create_pool(_DSN, min_size=1, max_size=2)
    try:
        await initialize_schema(pool, _cfg(catalog, data))
        async with pool.acquire() as c:
            assert await c.fetchval("SELECT to_regclass($1) IS NOT NULL", _q(data))
            # El conector nunca debe crear objetos en `public`.
            assert await c.fetchval(
                "SELECT to_regclass($1) IS NULL", data
            ), "la tabla se creó en el schema por defecto (public) en vez de en POSTGRES_SCHEMA"
            index = await c.fetchval(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = $2 AND tablename = $1 AND indexname LIKE 'uq_%_event_ts'",
                data, _SCHEMA,
            )
            assert index, "no se creó el índice único de deduplicación"
    finally:
        async with pool.acquire() as c:
            await _drop(c, data, catalog)
        await pool.close()


@pytest.mark.asyncio
async def test_initialize_schema_is_idempotent_with_compression_enabled():
    """Segundo arranque del conector sobre una hypertable ya comprimida.

    Regresión P0: TimescaleDB rechaza `CREATE UNIQUE INDEX` sobre hypertables con
    compresión activa incluso con `IF NOT EXISTS`, lo que hacía que cada reinicio
    del conector fallase en `initialize_schema` y entrase en crash-loop.
    """
    catalog, data = _names()
    pool = await asyncpg.create_pool(_DSN, min_size=1, max_size=2)
    cfg = _cfg(catalog, data)
    try:
        await initialize_schema(pool, cfg)
        async with pool.acquire() as c:
            compressed = await c.fetchval(
                "SELECT compression_enabled FROM timescaledb_information.hypertables "
                "WHERE hypertable_schema = $2 AND hypertable_name = $1",
                data, _SCHEMA,
            )
        assert compressed is True, "la hypertable no quedó comprimida: el test no reproduce el caso"

        # Lo que hace el conector en CADA arranque.
        await initialize_schema(pool, cfg)

        async with pool.acquire() as c:
            index = await c.fetchval(
                "SELECT indexname FROM pg_indexes "
                "WHERE schemaname = $2 AND tablename = $1 AND indexname LIKE 'uq_%_event_ts'",
                data, _SCHEMA,
            )
        assert index, "el índice único de deduplicación desapareció"
    finally:
        async with pool.acquire() as c:
            await _drop(c, data, catalog)
        await pool.close()


@pytest.mark.asyncio
async def test_duplicate_batch_is_idempotent():
    catalog, data, pool = await _prepare_schema()
    try:
        tag_id = await _tag_id(pool, catalog)
        batch = [_record(tag_id) for _ in range(5)]
        writer = BatchWriter(
            pool=pool,
            cfg=_writer_cfg(data, dedup=True),
            queue=None,
        )

        async with pool.acquire() as conn:
            first = await writer._insert_dedup(conn, batch)
            second = await writer._insert_dedup(conn, batch)
            total = await conn.fetchval(f"SELECT COUNT(*) FROM {_q(data)}")

        assert first == 5
        assert second == 0
        assert total == 5
    finally:
        async with pool.acquire() as conn:
            await _drop(conn, data, catalog)
        await pool.close()


@pytest.mark.asyncio
async def test_bad_status_code_quality_fits_int32():
    """Regresión: un StatusCode *Bad* (uint32 ≥ 0x80000000) desbordaba `quality`
    INTEGER y hacía fallar el COPY de todo el lote (`OverflowError`)."""
    catalog, data, pool = await _prepare_schema()
    try:
        tag_id = await _tag_id(pool, catalog)
        bad = ua.StatusCodes.BadNodeIdUnknown
        batch = [_record(tag_id), _record(tag_id, to_quality(bad))]
        writer = BatchWriter(
            pool=pool,
            cfg=_writer_cfg(data, dedup=True),
            queue=None,
        )

        async with pool.acquire() as conn:
            inserted = await writer._insert_dedup(conn, batch)
            stored = await conn.fetch(
                f"SELECT quality, (quality::bigint & 4294967295) AS unsigned "
                f"FROM {_q(data)} ORDER BY quality"
            )

        assert inserted == 2
        assert [row["quality"] for row in stored] == [to_quality(bad), 0]
        # El código OPC-UA original se recupera sin pérdida.
        assert stored[0]["unsigned"] == int(bad)
    finally:
        async with pool.acquire() as conn:
            await _drop(conn, data, catalog)
        await pool.close()


@pytest.mark.asyncio
async def test_dedup_disabled_falls_back_when_unique_index_exists():
    """Dedup apagada pero índice preexistente: el COPY falla y se degrada.

    Regresión del `NameError` que impedía ejecutar el fallback y dejaba el lote
    en un bucle de reintentos.
    """
    catalog, data, pool = await _prepare_schema()
    try:
        tag_id = await _tag_id(pool, catalog)
        batch = [_record(tag_id) for _ in range(4)]
        writer = BatchWriter(
            pool=pool,
            cfg=_writer_cfg(data, dedup=False),
            queue=None,
        )

        assert await writer._flush(batch, requeue_on_failure=False) is True
        # El segundo COPY viola el índice; debe caer al INSERT ... ON CONFLICT.
        assert await writer._flush(batch, requeue_on_failure=False) is True

        async with pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT COUNT(*) FROM {_q(data)}")
        assert total == len(batch)
    finally:
        async with pool.acquire() as conn:
            await _drop(conn, data, catalog)
        await pool.close()


@pytest.mark.asyncio
async def test_crash_between_replay_and_ack_does_not_duplicate(tmp_path):
    """Flujo completo: spill -> drain sin ACK -> reentrega -> dedup en BD."""
    catalog, data, pool = await _prepare_schema()
    try:
        tag_id = await _tag_id(pool, catalog)
        records = [_record(tag_id) for _ in range(4)]

        spill = SpillBuffer(_spill_cfg(tmp_path), "connector-acceptance")
        for record in records:
            assert spill.write(record)
        first_read = spill.drain(100)
        assert len(first_read.records) == 4
        # Caída lógica: se cierra sin ACK.
        spill.close()

        # Reinicio: los mismos registros se vuelven a entregar.
        spill2 = SpillBuffer(_spill_cfg(tmp_path), "connector-acceptance")
        try:
            second_read = spill2.drain(100)
            assert [r[0] for r in second_read.records] == [
                r[0] for r in records
            ]

            # El primer lote llegó a commit; el replay se intenta de nuevo.
            writer = BatchWriter(
                pool=pool,
                cfg=_writer_cfg(data, dedup=True),
                queue=None,
            )
            async with pool.acquire() as conn:
                await writer._insert_dedup(conn, first_read.records)
                await writer._insert_dedup(conn, second_read.records)
                total = await conn.fetchval(f"SELECT COUNT(*) FROM {_q(data)}")
            assert total == len(records)
        finally:
            spill2.close()
    finally:
        async with pool.acquire() as conn:
            await _drop(conn, data, catalog)
        await pool.close()
