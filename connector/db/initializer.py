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
        # Fail-closed con mensaje accionable: en PG15+ el schema public ya no
        # concede CREATE a PUBLIC. Solo se exige si hay tablas por crear.
        await _ensure_create_privilege(conn, catalog, data)
        await _create_catalog_table(conn, catalog)
        await _create_data_table(
            conn, data, catalog, cfg.use_timescale,
            dedup_enabled=getattr(cfg, "dedup_enabled", True),
        )
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


async def _ensure_create_privilege(conn: asyncpg.Connection, *tables: str) -> None:
    """Comprueba CREATE sobre el schema actual si falta alguna tabla por crear.

    Evita el error críptico ``permission denied for schema public`` en PG15+.
    Si las tablas ya existen no se exige CREATE: basta con los permisos DML.
    """
    missing: list[str] = []
    for name in tables:
        exists = await conn.fetchval("SELECT to_regclass($1) IS NOT NULL", name)
        if not exists:
            missing.append(name)
    if not missing:
        return
    schema = await conn.fetchval("SELECT current_schema()") or "public"
    can_create = await conn.fetchval(
        "SELECT has_schema_privilege(current_user, $1, 'CREATE')", schema
    )
    if not can_create:
        raise InitializationError(
            f"El usuario conector no puede crear tablas en el schema '{schema}' "
            f"(faltan: {', '.join(missing)}). "
            f"Ejecute como DBA: GRANT USAGE, CREATE ON SCHEMA {schema} TO <usuario_conector>;"
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


async def _create_data_table(
    conn: asyncpg.Connection,
    data: str,
    catalog: str,
    use_timescale: bool,
    *,
    dedup_enabled: bool = True,
) -> None:
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {data} (
            event_id      UUID,
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

    # Migración idempotente para tablas creadas antes de la Fase 2.
    await conn.execute(
        f"ALTER TABLE {data} ADD COLUMN IF NOT EXISTS event_id UUID;"
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

    # Clave de deduplicación: el replay desde disco es at-least-once y la
    # inserción usa ON CONFLICT (event_id, ts) DO NOTHING. El predicado deja
    # fuera las filas legadas (event_id NULL) sin penalizar el índice.
    if not dedup_enabled:
        log.info("dedup_disabled_skip_index", table=data)
    else:
        try:
            await conn.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{data}_event_ts "
                f"ON {data} (event_id, ts) WHERE event_id IS NOT NULL;"
            )
        except asyncpg.PostgresError as exc:
            raise InitializationError(
                "No se pudo crear la clave única de deduplicación "
                f"(event_id, ts) en '{data}': {exc}. "
                "POSTGRES_DEDUP_ENABLED=true requiere este índice. "
                "Revise permisos/estado de la hypertable o desactive la deduplicación."
            ) from exc

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
