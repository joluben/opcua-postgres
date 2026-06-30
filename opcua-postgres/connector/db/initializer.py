"""Inicialización idempotente del schema en TimescaleDB.

Secuencia (§8):
  1. Verifica que la extensión ``timescaledb`` esté instalada (si no, aborta).
  2. Crea la tabla de catálogo si no existe.
  3. Crea la tabla de datos + hypertable + índices + política de compresión si no existe.
  4. Verifica permisos mínimos del usuario conector.

El usuario conector NO crea extensiones ni usuarios: eso lo hace el DBA en el servidor de BD.
Los nombres de tabla provienen de configuración; se validan para evitar inyección.
"""

from __future__ import annotations

import re

import asyncpg

from ..config import DbConfig
from ..utils.logger import get_logger

log = get_logger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class InitializationError(RuntimeError):
    pass


def _validate_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise InitializationError(f"Nombre de tabla inválido: {name!r}")
    return name


async def initialize_schema(pool: asyncpg.Pool, cfg: DbConfig) -> None:
    catalog = _validate_ident(cfg.catalog_table)
    data = _validate_ident(cfg.data_table)

    async with pool.acquire() as conn:
        if cfg.use_timescale:
            await _ensure_timescaledb(conn)
        else:
            log.warning(
                "timescale_disabled",
                detail="POSTGRES_USE_TIMESCALE=false: PostgreSQL plano, sin hypertable ni compresión",
            )
        await _create_catalog_table(conn, catalog)
        await _create_data_table(conn, data, catalog, cfg.use_timescale)
        await _verify_permissions(conn, catalog, data)

    log.info("schema_initialized", catalog_table=catalog, data_table=data, timescale=cfg.use_timescale)


async def _ensure_timescaledb(conn: asyncpg.Connection) -> None:
    exists = await conn.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
    )
    if not exists:
        raise InitializationError(
            "La extensión TimescaleDB no está instalada en la base de datos remota. "
            "Debe instalarla el DBA: CREATE EXTENSION IF NOT EXISTS timescaledb;"
        )


async def _create_catalog_table(conn: asyncpg.Connection, catalog: str) -> None:
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog} (
            id            SERIAL PRIMARY KEY,
            node_id       TEXT        NOT NULL UNIQUE,
            display_name  TEXT,
            description   TEXT,
            data_type     TEXT,
            namespace_uri TEXT,
            active        BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )


async def _create_data_table(conn: asyncpg.Connection, data: str, catalog: str, use_timescale: bool) -> None:
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {data} (
            tag_id        INTEGER     NOT NULL REFERENCES {catalog}(id),
            ts            TIMESTAMPTZ NOT NULL,
            received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            value_num     DOUBLE PRECISION,
            value_str     TEXT,
            quality       INTEGER,
            connector_id  TEXT        NOT NULL
        );
        """
    )

    if use_timescale:
        await conn.execute(
            f"""
            SELECT create_hypertable(
                '{data}', 'ts',
                chunk_time_interval => INTERVAL '15 minutes',
                if_not_exists => TRUE
            );
            """
        )

    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{data}_tag_ts ON {data} (tag_id, ts DESC);"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{data}_quality ON {data} (quality) WHERE quality != 0;"
    )

    if not use_timescale:
        # PostgreSQL plano: sin compresión nativa. Considerar particionado declarativo manual.
        return

    # Política de compresión (datos > 7 días). Tolerante si ya existe.
    try:
        await conn.execute(
            f"ALTER TABLE {data} SET (timescaledb.compress, "
            f"timescaledb.compress_segmentby = 'tag_id');"
        )
        await conn.execute(
            f"SELECT add_compression_policy('{data}', INTERVAL '7 days', if_not_exists => TRUE);"
        )
    except asyncpg.PostgresError as exc:  # noqa: BLE001
        log.warning("compression_policy_skip", error=str(exc))


async def _verify_permissions(conn: asyncpg.Connection, catalog: str, data: str) -> None:
    """Comprueba INSERT en datos y UPDATE en catálogo (necesario para active/updated_at)."""
    checks = {
        f"{data}:INSERT": f"SELECT has_table_privilege('{data}', 'INSERT')",
        f"{catalog}:INSERT": f"SELECT has_table_privilege('{catalog}', 'INSERT')",
        f"{catalog}:UPDATE": f"SELECT has_table_privilege('{catalog}', 'UPDATE')",
    }
    for label, query in checks.items():
        granted = await conn.fetchval(query)
        if not granted:
            raise InitializationError(
                f"Permiso insuficiente para el usuario conector: falta {label}. "
                "Revise el GRANT en el servidor de BD (§8.2)."
            )
