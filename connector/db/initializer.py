"""Inicialización idempotente del schema en TimescaleDB.

Secuencia (§8):
  0. Verifica que el schema configurado (``POSTGRES_SCHEMA``) exista: lo crea el
     DBA; el conector nunca crea schemas.
  1. Verifica que la extensión ``timescaledb`` esté instalada (si no, aborta).
  2. Crea la tabla de catálogo si no existe.
  3. Crea la tabla de datos + hypertable + índices y, si están configuradas,
     las políticas de compresión y retención (con posibilidad de delegarlas al DBA).
  4. Verifica permisos mínimos del usuario conector.

El usuario conector NO crea extensiones ni usuarios: eso lo hace el DBA en el servidor de BD.
El schema y los nombres de tabla provienen de configuración; se validan y se citan
para evitar inyección, y todos los objetos se cualifican con el schema indicado
(nunca se depende del ``search_path``).
"""

from __future__ import annotations

import asyncio
import re

import asyncpg

from ..config import DbConfig
from ..utils.ident import IdentifierError, qualified_name, quote_ident, validate_ident
from ..utils.logger import get_logger

log = get_logger(__name__)

# Se interpola en SQL: solo se acepta "<n> <unidad>".
_INTERVAL_RE = re.compile(r"^\d+\s+(hour|day|week|month|year)s?$", re.IGNORECASE)


class InitializationError(RuntimeError):
    pass


def _validate_ident(name: str, what: str = "Nombre de tabla") -> str:
    try:
        return validate_ident(name)
    except IdentifierError as exc:
        raise InitializationError(f"{what} inválido: {name!r}") from exc


def _validate_interval(name: str, value: str | None) -> str | None:
    """Valida un intervalo de TimescaleDB antes de interpolarlo en SQL.

    ``None`` es válido y significa "no gestionar la política desde el conector".
    """
    if value is None:
        return None
    if not _INTERVAL_RE.match(value):
        raise InitializationError(
            f"Intervalo {name} inválido: {value!r}. "
            "Formato esperado: '<n> hour|day|week|month|year' (p.ej. '30 days')."
        )
    return value


# Reintentos ante deadlock en el DDL inicial. TimescaleDB puede detectar un
# deadlock cuando el DDL (ALTER TABLE / CREATE INDEX) coincide con su job de
# compresión en la misma hypertable: el job retiene el lock de tabla y el DDL
# pide el lock de relación (o al revés). Es transitorio por definición y en
# producción ocurre al arrancar dos conectores a la vez sobre la misma BD.
_INIT_MAX_ATTEMPTS = 3
_INIT_RETRY_BASE_S = 0.5


async def initialize_schema(pool: asyncpg.Pool, cfg: DbConfig) -> None:
    schema = _validate_ident(cfg.schema, "Schema")
    catalog = _validate_ident(cfg.catalog_table)
    data = _validate_ident(cfg.data_table)

    for attempt in range(1, _INIT_MAX_ATTEMPTS + 1):
        try:
            await _initialize_once(pool, cfg, schema, catalog, data)
            return
        except asyncpg.DeadlockDetectedError as exc:
            if attempt >= _INIT_MAX_ATTEMPTS:
                raise
            log.warning(
                "schema_init_deadlock_retry",
                attempt=attempt,
                schema=schema,
                table=data,
                error=str(exc),
            )
            await asyncio.sleep(_INIT_RETRY_BASE_S * attempt)


async def _initialize_once(
    pool: asyncpg.Pool, cfg: DbConfig, schema: str, catalog: str, data: str
) -> None:
    async with pool.acquire() as conn:
        await _ensure_schema(conn, schema)
        if cfg.use_timescale:
            await _ensure_timescaledb(conn)
        else:
            log.warning(
                "timescale_disabled",
                detail="POSTGRES_USE_TIMESCALE=false: PostgreSQL plano, sin hypertable ni compresión",
            )
        # Fail-closed con mensaje accionable: en PG15+ el schema public ya no
        # concede CREATE a PUBLIC. Solo se exige si hay tablas por crear.
        await _ensure_create_privilege(conn, schema, catalog, data)
        await _create_catalog_table(conn, schema, catalog)
        await _create_data_table(
            conn, data, catalog, cfg.use_timescale,
            schema=schema,
            dedup_enabled=getattr(cfg, "dedup_enabled", True),
            chunk_interval=getattr(cfg, "timescale_chunk_interval", "1 day"),
            compression_interval=getattr(
                cfg, "timescale_compression_interval", "1 month"
            ),
            retention_interval=getattr(
                cfg, "timescale_retention_interval", "365 days"
            ),
        )
        await _verify_permissions(conn, schema, catalog, data)

    log.info(
        "schema_initialized",
        schema=schema,
        catalog_table=catalog,
        data_table=data,
        timescale=cfg.use_timescale,
    )


async def _ensure_schema(conn: asyncpg.Connection, schema: str) -> None:
    """Verifica que el schema configurado exista.

    ``POSTGRES_SCHEMA`` es obligatorio y el conector **no** crea schemas (el rol
    de aplicación no tiene privilegios de creación sobre la base de datos): si
    falta, se registra un error accionable y la inicialización falla en cerrado.
    """
    exists = await conn.fetchval(
        "SELECT to_regnamespace($1) IS NOT NULL", schema
    )
    if exists:
        return
    quoted = quote_ident(schema)
    log.error("schema_missing", schema=schema)
    raise InitializationError(
        f"El schema '{schema}' no existe. El conector no crea schemas: "
        f"ejecute como DBA: CREATE SCHEMA IF NOT EXISTS {quoted} AUTHORIZATION "
        f"<usuario_conector>; y GRANT USAGE, CREATE ON SCHEMA {quoted} TO "
        "<usuario_conector>;"
    )


async def _ensure_timescaledb(conn: asyncpg.Connection) -> None:
    exists = await conn.fetchval(
        "SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"
    )
    if not exists:
        raise InitializationError(
            "La extensión TimescaleDB no está instalada en la base de datos remota. "
            "Debe instalarla el DBA: CREATE EXTENSION IF NOT EXISTS timescaledb;"
        )


async def _ensure_create_privilege(
    conn: asyncpg.Connection, schema: str, *tables: str
) -> None:
    """Comprueba CREATE sobre el schema configurado si falta alguna tabla.

    Evita el error críptico ``permission denied for schema <schema>`` en PG15+.
    Si las tablas ya existen no se exige CREATE: basta con los permisos DML.
    """
    missing: list[str] = []
    for name in tables:
        exists = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL", qualified_name(schema, name)
        )
        if not exists:
            missing.append(name)
    if not missing:
        return
    can_create = await conn.fetchval(
        "SELECT has_schema_privilege(current_user, $1, 'CREATE')", schema
    )
    if not can_create:
        raise InitializationError(
            f"El usuario conector no puede crear tablas en el schema '{schema}' "
            f"(faltan: {', '.join(missing)}). "
            f"Ejecute como DBA: GRANT USAGE, CREATE ON SCHEMA {quote_ident(schema)} "
            "TO <usuario_conector>;"
        )


async def _create_catalog_table(
    conn: asyncpg.Connection, schema: str, catalog: str
) -> None:
    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_name(schema, catalog)} (
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
    schema: str,
    dedup_enabled: bool = True,
    chunk_interval: str | None = "1 day",
    compression_interval: str | None = "1 month",
    retention_interval: str | None = "365 days",
) -> None:
    # Validar antes de cualquier DDL: si un intervalo es inválido no debe
    # haberse creado parte del schema.
    if use_timescale:
        chunk_interval = _validate_interval("chunk_interval", chunk_interval)
        compression_interval = _validate_interval(
            "compression_interval", compression_interval
        )
        retention_interval = _validate_interval(
            "retention_interval", retention_interval
        )

    # Todos los objetos (tabla, índice, hypertable y políticas) se cualifican con
    # el schema configurado: nada depende del search_path de la sesión.
    data_sql = qualified_name(schema, data)
    catalog_sql = qualified_name(schema, catalog)
    # Las funciones de TimescaleDB reciben el nombre como regclass en un literal:
    # se pasa la forma citada para que respete mayúsculas y el schema explícito.
    data_regclass = f"'{data_sql}'"

    await conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {data_sql} (
            event_id      UUID,
            tag_id        INTEGER     NOT NULL REFERENCES {catalog_sql}(id),
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
        f"ALTER TABLE {data_sql} ADD COLUMN IF NOT EXISTS event_id UUID;"
    )

    if use_timescale:
        if chunk_interval is None:
            # Sin intervalo explícito: TimescaleDB aplica su default y el DBA
            # puede ajustarlo después con set_chunk_time_interval().
            await conn.execute(
                f"""
                SELECT create_hypertable(
                    {data_regclass}, 'ts',
                    if_not_exists => TRUE
                );
                """
            )
            log.info(
                "timescale_chunk_interval_external",
                table=data,
                detail="Intervalo de chunk no gestionado por el conector.",
            )
        else:
            await conn.execute(
                f"""
                SELECT create_hypertable(
                    {data_regclass}, 'ts',
                    chunk_time_interval => INTERVAL '{chunk_interval}',
                    if_not_exists => TRUE
                );
                """
            )

    # Los nombres de índice también se citan: así respetan el nombre exacto de la
    # tabla (p. ej. ``MyData``) y coinciden con la comprobación de existencia de
    # más abajo, que consulta el nombre cualificado y citado.
    tag_index = quote_ident(f"idx_{data}_tag_ts")
    quality_index = quote_ident(f"idx_{data}_quality")
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS {tag_index} ON {data_sql} (tag_id, ts DESC);"
    )
    await conn.execute(
        f"CREATE INDEX IF NOT EXISTS {quality_index} ON {data_sql} (quality) WHERE quality != 0;"
    )

    # Clave de deduplicación: el replay desde disco es at-least-once y la
    # inserción usa ON CONFLICT (event_id, ts) DO NOTHING. El predicado deja
    # fuera las filas legadas (event_id NULL) sin penalizar el índice.
    #
    # P0: TimescaleDB rechaza CREATE [UNIQUE] INDEX —incluso con IF NOT EXISTS—
    # sobre una hypertable con compresión activada: su hook aborta antes de
    # comprobar si el índice ya existe ("operation not supported on hypertables
    # that have compression enabled"). Como la compresión se activa al final de
    # esta misma función, a partir del segundo arranque el índice ya está creado
    # y emitir el CREATE hacía fallar initialize_schema en bucle: el contenedor
    # reiniciaba sin llegar a ingerir. Por eso se consulta el catálogo primero.
    if not dedup_enabled:
        log.info("dedup_disabled_skip_index", table=data)
    else:
        index_name = f"uq_{data}_event_ts"
        index_sql = quote_ident(index_name)
        index_exists = await conn.fetchval(
            "SELECT to_regclass($1) IS NOT NULL",
            qualified_name(schema, index_name),
        )
        if index_exists:
            log.info("dedup_index_present", table=data, index=index_name)
        else:
            try:
                await conn.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {index_sql} "
                    f"ON {data_sql} (event_id, ts) WHERE event_id IS NOT NULL;"
                )
            except asyncpg.PostgresError as exc:
                raise InitializationError(
                    "No se pudo crear la clave única de deduplicación "
                    f"(event_id, ts) en '{data}': {exc}. "
                    "POSTGRES_DEDUP_ENABLED=true requiere este índice. "
                    "Si la hypertable ya tiene compresión activa, el DBA debe "
                    "descomprimir sus chunks y crear el índice antes de volver "
                    "a habilitarla (o desactivar la deduplicación)."
                ) from exc

    if not use_timescale:
        # PostgreSQL plano: sin compresión nativa. Considerar particionado declarativo manual.
        return

    if compression_interval is None and retention_interval is None:
        log.info(
            "timescale_policies_managed_externally",
            table=data,
            detail=(
                "Compresión y retención desactivadas en el conector: "
                "las gestiona el DBA directamente en la base de datos."
            ),
        )
        return

    # Se aplican por separado para que un fallo en compresión no impida
    # aplicar la retención (y viceversa). Tolerante si la política ya existe.
    if compression_interval is not None:
        try:
            await conn.execute(
                f"ALTER TABLE {data_sql} SET (timescaledb.compress, "
                f"timescaledb.compress_segmentby = 'tag_id');"
            )
            await conn.execute(
                f"SELECT add_compression_policy({data_regclass}, "
                f"INTERVAL '{compression_interval}', if_not_exists => TRUE);"
            )
            log.info(
                "timescale_compression_policy_applied",
                table=data,
                interval=compression_interval,
            )
        except asyncpg.PostgresError as exc:  # noqa: BLE001
            log.warning(
                "timescale_compression_policy_skip",
                table=data,
                interval=compression_interval,
                error=str(exc),
            )

    if retention_interval is not None:
        try:
            await conn.execute(
                f"SELECT add_retention_policy({data_regclass}, "
                f"INTERVAL '{retention_interval}', if_not_exists => TRUE);"
            )
            log.info(
                "timescale_retention_policy_applied",
                table=data,
                interval=retention_interval,
            )
        except asyncpg.PostgresError as exc:  # noqa: BLE001
            log.warning(
                "timescale_retention_policy_skip",
                table=data,
                interval=retention_interval,
                error=str(exc),
            )


async def _verify_permissions(
    conn: asyncpg.Connection, schema: str, catalog: str, data: str
) -> None:
    """Comprueba INSERT en datos y UPDATE en catálogo (necesario para active/updated_at)."""
    checks = {
        f"{schema}.{data}:INSERT": (qualified_name(schema, data), "INSERT"),
        f"{schema}.{catalog}:INSERT": (qualified_name(schema, catalog), "INSERT"),
        f"{schema}.{catalog}:UPDATE": (qualified_name(schema, catalog), "UPDATE"),
    }
    for label, (table, privilege) in checks.items():
        granted = await conn.fetchval(
            "SELECT has_table_privilege($1, $2)", table, privilege
        )
        if not granted:
            raise InitializationError(
                f"Permiso insuficiente para el usuario conector: falta {label}. "
                "Revise el GRANT en el servidor de BD (§8.2)."
            )
