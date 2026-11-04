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

import os
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import asyncpg
import pytest

from connector.db.initializer import initialize_schema
from connector.db.spill import SpillBuffer
from connector.db.writer import BatchWriter

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.getenv("ACCEPTANCE_CONNECTOR_DSN"),
        reason="requiere ACCEPTANCE_CONNECTOR_DSN (ver scripts/acceptance_pg.sh)",
    ),
]

_DSN = os.getenv("ACCEPTANCE_CONNECTOR_DSN", "")
_ADMIN_DSN = os.getenv("ACCEPTANCE_ADMIN_DSN") or _DSN


def _names() -> tuple[str, str]:
    token = uuid.uuid4().hex[:12]
    return f"acc_catalog_{token}", f"acc_data_{token}"


def _cfg(catalog: str, data: str) -> SimpleNamespace:
    return SimpleNamespace(
        use_timescale=True,
        catalog_table=catalog,
        data_table=data,
        dedup_enabled=True,
    )


async def _drop(conn: asyncpg.Connection, data: str, catalog: str) -> None:
    await conn.execute(f"DROP TABLE IF EXISTS {data} CASCADE")
    await conn.execute(f"DROP TABLE IF EXISTS {catalog} CASCADE")


async def _prepare_schema() -> tuple[str, str, asyncpg.Pool]:
    catalog, data = _names()
    pool = await asyncpg.create_pool(_DSN, min_size=1, max_size=2)
    await initialize_schema(pool, _cfg(catalog, data))
    return catalog, data, pool


async def _tag_id(pool: asyncpg.Pool, catalog: str) -> int:
    async with pool.acquire() as conn:
        return await conn.fetchval(
            f"INSERT INTO {catalog} (node_id, display_name) "
            f"VALUES ('ns=2;s=Acceptance', 'Acceptance') RETURNING id"
        )


def _spill_cfg(tmp_path) -> SimpleNamespace:
    return SimpleNamespace(
        spill_enabled=True,
        spill_dir=str(tmp_path),
        spill_max_mb=10,
        spill_segment_mb=1,
        spill_drain_timeout_s=5,
    )


def _record(tag_id: int) -> tuple:
    return (
        uuid.uuid4(), tag_id, datetime.now(timezone.utc), 1.0, None, 0,
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
            assert await c.fetchval("SELECT to_regclass($1) IS NOT NULL", data)
            index = await c.fetchval(
                "SELECT indexname FROM pg_indexes "
                "WHERE tablename = $1 AND indexname LIKE 'uq_%_event_ts'",
                data,
            )
            assert index, "no se creó el índice único de deduplicación"
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
            cfg=SimpleNamespace(
                data_table=data, batch_size=100, flush_interval_ms=500,
                dedup_enabled=True,
            ),
            queue=None,
        )

        async with pool.acquire() as conn:
            first = await writer._insert_dedup(conn, batch)
            second = await writer._insert_dedup(conn, batch)
            total = await conn.fetchval(f"SELECT COUNT(*) FROM {data}")

        assert first == 5
        assert second == 0
        assert total == 5
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
                cfg=SimpleNamespace(
                    data_table=data, batch_size=100, flush_interval_ms=500,
                    dedup_enabled=True,
                ),
                queue=None,
            )
            async with pool.acquire() as conn:
                await writer._insert_dedup(conn, first_read.records)
                await writer._insert_dedup(conn, second_read.records)
                total = await conn.fetchval(f"SELECT COUNT(*) FROM {data}")
            assert total == len(records)
        finally:
            spill2.close()
    finally:
        async with pool.acquire() as conn:
            await _drop(conn, data, catalog)
        await pool.close()
