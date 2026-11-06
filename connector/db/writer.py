"""Batch writer: acumula registros de la cola y los inserta con COPY.

Pipeline (§9.2):
    asyncio.Queue  ->  Batch Accumulator (BATCH_SIZE o FLUSH_INTERVAL_MS)  ->  COPY FROM

P0:
- Captura amplia de excepciones (antes solo PostgresError → pérdida de batch).
- Backoff breve tras fallo para no hacer hot-loop contra una BD caída.
- Métrica BATCH_LAG (ts más antiguo → flush) para detectar backlog.
- Soporte multi-writer: N instancias consumen la misma cola; solo worker 0
  hace replay de spill (evita duplicados).

Cada registro es una tupla lista para COPY (``received_at`` lo rellena el
DEFAULT NOW() de la BD):
    (event_id, tag_id, ts, value_num, value_str, quality, connector_id)

Con ``POSTGRES_DEDUP_ENABLED=true`` la inserción pasa por una tabla temporal y
usa ``INSERT ... ON CONFLICT (event_id, ts) DO NOTHING``: el replay del spill es
at-least-once, pero la clave única lo hace idempotente.
"""

from __future__ import annotations

import asyncio
import itertools
import re
import time
import uuid
from datetime import datetime, timezone
from typing import List, Tuple

import asyncpg

from ..config import DbConfig
from ..utils import metrics
from ..utils.logger import get_logger
from .spill import SpillBuffer, enqueue_or_spill

log = get_logger(__name__)

Record = Tuple[uuid.UUID, int, object, object, object, int, str]

_COLUMNS = [
    "event_id", "tag_id", "ts", "value_num", "value_str", "quality",
    "connector_id",
]
_COLUMNS_SQL = ", ".join(_COLUMNS)

# Tabla TEMP de staging. Las tablas temporales son por sesión, pero la conexión
# se reutiliza desde el pool: por eso cada intento usa un nombre único (contador
# monotónico + id de worker). Así un staging huérfano de un intento anterior
# (COPY cancelado, conexión rota a mitad de transacción) nunca se reutiliza ni
# colisiona con el intento siguiente, y no hace falta IF NOT EXISTS.
#
# ON COMMIT DROP se evalúa en el servidor al terminar la transacción, tanto en
# COMMIT como en ROLLBACK: es la garantía primaria y no depende de que Python
# llegue a ejecutar código de limpieza (cancelación de tarea incluida).
_STAGE_TABLE_PREFIX = "_opc_ingest_stage"
_STAGE_DDL = """
    CREATE TEMP TABLE {name} (
        event_id     UUID,
        tag_id       INTEGER,
        ts           TIMESTAMPTZ,
        value_num    DOUBLE PRECISION,
        value_str    TEXT,
        quality      INTEGER,
        connector_id TEXT
    ) ON COMMIT DROP
"""
_stage_seq = itertools.count(1)
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ident(name: str) -> str:
    """Valida un identificador generado internamente antes de interpolarlo en SQL."""
    if not _IDENT_RE.match(name):
        raise ValueError(f"identificador SQL inválido: {name!r}")
    return name


def _new_stage_table(worker_id: int = 0) -> str:
    """Nombre único de staging para un único intento de inserción.

    El sufijo hexadecimal crece de forma monotónica: dos intentos sobre la misma
    conexión del pool nunca comparten tabla, aunque el anterior no se limpiara.
    """
    return _ident(f"{_STAGE_TABLE_PREFIX}_w{int(worker_id)}_{next(_stage_seq):x}")


def _rows_affected(status: str) -> int:
    """Extrae el número de filas de un ``INSERT 0 N`` de asyncpg."""
    try:
        return int(status.rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        return 0


def _batch_lag_s(batch: List[Record]) -> float:
    """Retraso del registro más antiguo del lote (0 si no se puede calcular)."""
    oldest: datetime | None = None
    for r in batch:
        ts = r[2] if len(r) > 2 else None
        if isinstance(ts, datetime):
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            if oldest is None or ts < oldest:
                oldest = ts
    if oldest is None:
        return 0.0
    return max(0.0, (datetime.now(timezone.utc) - oldest).total_seconds())


class BatchWriter:
    def __init__(
        self,
        pool: asyncpg.Pool,
        cfg: DbConfig,
        queue: "asyncio.Queue[Record]",
        spill: SpillBuffer | None = None,
        worker_id: int = 0,
        health: "metrics.HealthState | None" = None,
    ) -> None:
        self._pool = pool
        self._cfg = cfg
        self._queue = queue
        self._spill = spill
        self._health = health
        self._table = getattr(cfg, "data_table", "opc_raw_values")
        self._batch_size = int(getattr(cfg, "batch_size", 1000))
        self._flush_s = float(getattr(cfg, "flush_interval_ms", 500)) / 1000.0
        self._stop = asyncio.Event()
        self._db_healthy = True
        self._worker_id = worker_id
        self._dedup = bool(getattr(cfg, "dedup_enabled", True))
        # Solo el worker 0 reinyecta spill (evita drenados concurrentes duplicados).
        self._is_spill_leader = worker_id == 0

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        flush_s = self._flush_s
        batch: List[Record] = []
        last_flush = time.monotonic()

        while not self._stop.is_set():
            timeout = max(0.0, flush_s - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout or flush_s)
                batch.append(item)
            except asyncio.TimeoutError:
                pass

            try:
                metrics.QUEUE_SIZE.set(self._queue.qsize())
            except Exception:  # noqa: BLE001
                pass

            # Reinyectar spill solo desde el líder. Con el rediseño has_data() es
            # O(1) y sin I/O (cola interna + flag cacheado), así que se consulta en
            # cada iteración: no se limita el throughput de replay.
            await self._replay_spill()

            full = len(batch) >= self._batch_size
            due = (time.monotonic() - last_flush) >= flush_s
            if batch and (full or due):
                await self._flush(batch)
                batch = []
                last_flush = time.monotonic()

        await self._final_drain(batch)

    async def _replay_spill(self) -> None:
        """Relee un lote del spill y solo lo confirma si el commit tuvo éxito.

        En fallo se hace NACK: el lote sigue en disco y se reintenta en el
        siguiente ciclo (no se reencola en memoria para no duplicarlo).
        """
        if not self._is_spill_leader or not self._db_healthy or self._spill is None:
            return
        replay = None
        try:
            if not await self._spill.has_data_async():
                return
            replay = await self._spill.drain_async(self._batch_size)
            if not replay:
                return
            ok = await self._flush(replay.records, requeue_on_failure=False)
            if ok:
                self._spill.ack(replay.tokens)
            else:
                self._spill.nack(replay.tokens)
                replay = None
        except Exception as exc:  # noqa: BLE001
            log.warning("spill_replay_failed", error=str(exc),
                        worker=self._worker_id)
            # Liberar el lote en vuelo para que se pueda releer en el próximo ciclo.
            if replay is not None:
                self._spill.nack(replay.tokens)

    async def _final_drain(self, batch: List[Record]) -> None:
        """Volcado final acotado: lo pendiente acaba en BD o en el spill.

        Solo se intenta un último COPY con el lote local ya acumulado. El resto
        de la cola se persiste en disco directamente: en apagado no se debe
        depender de una BD sana ni cancelar un COPY en vuelo (lo que dejaría el
        resultado del commit en un estado desconocido).
        """
        if batch:
            ok = await self._flush(batch, requeue_on_failure=False)
            if not ok:
                self._spill_or_drop(batch)
        self._spill_queue_remainder()

    def _spill_queue_remainder(self) -> None:
        """Persiste en disco lo que quede en la cola, sin tocar la BD.

        ``spill.write()`` es O(1) (solo encola), así que vaciar la cola es rápido.
        """
        while True:
            rest: List[Record] = []
            while len(rest) < self._batch_size:
                try:
                    rest.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            if not rest:
                return
            self._spill_or_drop(rest)

    def _spill_or_drop(self, batch: List[Record]) -> None:
        """Último recurso de apagado: spill; si no hay spill, descartar con métrica."""
        for item in batch:
            if self._spill is not None and self._spill.write(item):
                continue
            try:
                metrics.VALUES_DROPPED.inc()
                metrics.SHUTDOWN_DROPPED.inc()
            except Exception:  # noqa: BLE001
                pass

    async def _flush(
        self, batch: List[Record], *, requeue_on_failure: bool = True
    ) -> bool:
        try:
            lag = _batch_lag_s(batch)
            try:
                metrics.BATCH_LAG.set(lag)
            except Exception:  # noqa: BLE001
                pass
        except Exception:  # noqa: BLE001
            lag = 0.0

        start = time.perf_counter()
        try:
            written = len(batch)
            async with self._pool.acquire() as conn:
                if self._dedup:
                    written = await self._insert_dedup(conn, batch)
                else:
                    try:
                        await conn.copy_records_to_table(
                            self._table, records=batch, columns=_COLUMNS
                        )
                    except asyncpg.UniqueViolationError:
                        # Puede quedar un índice único de una ejecución previa
                        # con dedup habilitada: degradar a inserción idempotente
                        # en vez de reencolar el lote en bucle.
                        log.warning(
                            "copy_unique_violation_fallback_dedup",
                            worker=self._worker_id,
                        )
                        written = await self._insert_dedup(conn, batch)
            duration = time.perf_counter() - start
            try:
                metrics.WRITE_LATENCY.observe(duration)
                metrics.VALUES_WRITTEN.inc(written)
                metrics.DB_STATUS.set(1)
            except Exception:  # noqa: BLE001
                pass
            self._db_healthy = True
            if self._health is not None:
                self._health.db_connected = True
            log.info(
                "batch_written",
                rows=written,
                received=len(batch),
                duration_ms=round(duration * 1000, 1),
                lag_s=round(lag, 2),
                table=self._table,
                worker=self._worker_id,
            )
            return True
        except Exception as exc:  # P0: antes solo PostgresError → batch perdido
            try:
                metrics.DB_ERRORS.inc()
                metrics.DB_STATUS.set(0)
            except Exception:  # noqa: BLE001
                pass
            self._db_healthy = False
            if self._health is not None:
                # /health no puede seguir devolviendo 200 si las escrituras
                # fallan, aunque la conexión del pool siga viva.
                self._health.db_connected = False
            log.error("batch_write_failed", rows=len(batch), error=str(exc),
                      error_type=type(exc).__name__, worker=self._worker_id)
            if requeue_on_failure:
                # No perder datos: re-encolar y, si la cola está llena, volcar a disco.
                self._requeue(batch)
                # P0: pausa breve para no saturar una BD caída en hot-loop.
                # Varios writers comparten el backoff (jitter por worker).
                await asyncio.sleep(0.5 + 0.1 * self._worker_id)
            return False

    async def _insert_dedup(self, conn: asyncpg.Connection, batch: List[Record]) -> int:
        """Copia a una tabla TEMP e inserta con ``ON CONFLICT DO NOTHING``.

        Todo ocurre en una única transacción. Seguridad del staging cuando
        conviven varios writers (``POSTGRES_NUM_WRITERS > 1``) y las conexiones
        se reciclan desde el pool:

        1. Nombre único por intento (``_new_stage_table``): un intento posterior
           en la misma conexión física no puede reutilizar una tabla huérfana.
        2. ``ON COMMIT DROP``: PostgreSQL elimina la tabla al CONFIRMAR y al
           REVERTIR, así que un COPY fallido o una sentencia cancelada no deja
           restos, aunque la tarea se cancele o la conexión se corte después.
        3. ``DROP TABLE IF EXISTS`` en el ``finally``: red de seguridad para el
           caso en que el bloque transaccional no llegó a finalizar (conexión
           caída, cancelación durante el ``COMMIT``). Es *best-effort*: nunca
           propaga su propio error para no enmascarar el original; si la
           conexión está rota, el pool la descarta y con ella su sesión.

        Devuelve las filas realmente insertadas.
        """
        stage = _new_stage_table(self._worker_id)
        committed = False
        try:
            async with conn.transaction():
                await conn.execute(_STAGE_DDL.format(name=stage))
                await conn.copy_records_to_table(
                    stage, records=batch, columns=_COLUMNS
                )
                inserted_rows = await conn.fetch(
                    f"INSERT INTO {self._table} ({_COLUMNS_SQL}) "
                    f"SELECT {_COLUMNS_SQL} FROM {stage} "
                    f"ON CONFLICT (event_id, ts) WHERE event_id IS NOT NULL "
                    f"DO NOTHING RETURNING 1"
                )
            committed = True
        finally:
            # Si el COMMIT se completó, ON COMMIT DROP ya eliminó la tabla: no se
            # gasta un round-trip extra en el camino feliz.
            if not committed:
                await self._drop_stage(conn, stage)
        inserted = len(inserted_rows)
        duplicates = len(batch) - inserted
        if duplicates > 0:
            try:
                metrics.VALUES_DUPLICATES.inc(duplicates)
            except Exception:  # noqa: BLE001
                pass
        return inserted

    async def _drop_stage(self, conn: asyncpg.Connection, stage: str) -> None:
        """Elimina la tabla de staging fuera de la transacción (best-effort).

        Se ejecuta después de que el contexto transaccional haya terminado, por
        lo que la conexión ya está en autocommit (o descartada por el pool): un
        ``DROP`` sobre una transacción abortada no tendría efecto. Los errores
        se registran y se ignoran deliberadamente; la garantía fuerte la da
        ``ON COMMIT DROP``. ``CancelledError`` se propaga para no romper la
        semántica de cancelación de ``wait_for``/``timeout``.
        """
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {_ident(stage)}")
        except asyncio.CancelledError:
            log.warning("stage_table_drop_cancelled", table=stage,
                        worker=self._worker_id)
            raise
        except Exception as exc:  # noqa: BLE001
            log.warning("stage_table_drop_failed", table=stage,
                        worker=self._worker_id, error=str(exc),
                        error_type=type(exc).__name__)

    def _requeue(self, batch: List[Record]) -> None:
        for item in batch:
            enqueue_or_spill(self._queue, item, self._spill)
