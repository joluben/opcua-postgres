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
    (tag_id, ts, value_num, value_str, quality, connector_id)
"""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import List, Tuple

import asyncpg

from ..config import DbConfig
from ..utils import metrics
from ..utils.logger import get_logger
from .spill import SpillBuffer, enqueue_or_spill

log = get_logger(__name__)

Record = Tuple[int, object, object, object, int, str]

_COLUMNS = ["tag_id", "ts", "value_num", "value_str", "quality", "connector_id"]


def _batch_lag_s(batch: List[Record]) -> float:
    """Retraso del registro más antiguo del lote (0 si no se puede calcular)."""
    oldest: datetime | None = None
    for r in batch:
        ts = r[1] if len(r) > 1 else None
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
    ) -> None:
        self._pool = pool
        self._cfg = cfg
        self._queue = queue
        self._spill = spill
        self._table = getattr(cfg, "data_table", "opc_raw_values")
        self._batch_size = int(getattr(cfg, "batch_size", 1000))
        self._flush_s = float(getattr(cfg, "flush_interval_ms", 500)) / 1000.0
        self._stop = asyncio.Event()
        self._db_healthy = True
        self._worker_id = worker_id
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
            if self._is_spill_leader and self._db_healthy and self._spill is not None:
                try:
                    if await self._spill.has_data_async():
                        replay = await self._spill.drain_async(self._batch_size)
                        if replay:
                            await self._flush(replay)
                except Exception as exc:  # noqa: BLE001
                    log.warning("spill_replay_failed", error=str(exc),
                                worker=self._worker_id)

            full = len(batch) >= self._batch_size
            due = (time.monotonic() - last_flush) >= flush_s
            if batch and (full or due):
                await self._flush(batch)
                batch = []
                last_flush = time.monotonic()

        if batch:
            await self._flush(batch)

    async def _flush(self, batch: List[Record]) -> None:
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
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    self._table, records=batch, columns=_COLUMNS
                )
            duration = time.perf_counter() - start
            try:
                metrics.WRITE_LATENCY.observe(duration)
                metrics.VALUES_WRITTEN.inc(len(batch))
                metrics.DB_STATUS.set(1)
            except Exception:  # noqa: BLE001
                pass
            self._db_healthy = True
            log.info(
                "batch_written",
                rows=len(batch),
                duration_ms=round(duration * 1000, 1),
                lag_s=round(lag, 2),
                table=self._table,
                worker=self._worker_id,
            )
        except Exception as exc:  # P0: antes solo PostgresError → batch perdido
            try:
                metrics.DB_ERRORS.inc()
                metrics.DB_STATUS.set(0)
            except Exception:  # noqa: BLE001
                pass
            self._db_healthy = False
            # No perder datos: re-encolar y, si la cola está llena, volcar a disco (spill).
            log.error("batch_write_failed", rows=len(batch), error=str(exc),
                      error_type=type(exc).__name__, worker=self._worker_id)
            self._requeue(batch)
            # P0: pausa breve para no saturar una BD caída en hot-loop.
            # Varios writers comparten el backoff (jitter por worker).
            await asyncio.sleep(0.5 + 0.1 * self._worker_id)

    def _requeue(self, batch: List[Record]) -> None:
        for item in batch:
            enqueue_or_spill(self._queue, item, self._spill)
