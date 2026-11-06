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

# Tabla TEMP por conexión (cada conexión del pool tiene la suya). Se crea
# dentro de una transacción con ON COMMIT DROP: al confirmar el lote desaparece,
# sin necesidad de TRUNCATE ni de limpiar restos de un intento fallido.
_STAGE_TABLE = "_opc_ingest_stage"
_STAGE_DDL = f"""
    CREATE TEMP TABLE IF NOT EXISTS {_STAGE_TABLE} (
        event_id     UUID,
        tag_id       INTEGER,
        ts           TIMESTAMPTZ,
        value_num    DOUBLE PRECISION,
        value_str    TEXT,
        quality      INTEGER,
        connector_id TEXT
    ) ON COMMIT DROP
"""


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

        Todo ocurre en una única transacción: la tabla de staging se crea con
        ``ON COMMIT DROP`` y desaparece al confirmar. Si el COPY o el INSERT
        fallan, la transacción revierte y no queda estado parcial en la conexión.
        Devuelve las filas realmente insertadas.
        """
        async with conn.transaction():
            await conn.execute(_STAGE_DDL)
            await conn.copy_records_to_table(
                _STAGE_TABLE, records=batch, columns=_COLUMNS
            )
            inserted_rows = await conn.fetch(
                f"INSERT INTO {self._table} ({_COLUMNS_SQL}) "
                f"SELECT {_COLUMNS_SQL} FROM {_STAGE_TABLE} "
                f"ON CONFLICT (event_id, ts) WHERE event_id IS NOT NULL DO NOTHING "
                f"RETURNING 1"
            )
        inserted = len(inserted_rows)
        duplicates = len(batch) - inserted
        if duplicates > 0:
            try:
                metrics.VALUES_DUPLICATES.inc(duplicates)
            except Exception:  # noqa: BLE001
                pass
        return inserted

    def _requeue(self, batch: List[Record]) -> None:
        for item in batch:
            enqueue_or_spill(self._queue, item, self._spill)
