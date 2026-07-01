"""Batch writer: acumula registros de la cola y los inserta con COPY.

Pipeline (§9.2):
    asyncio.Queue  ->  Batch Accumulator (BATCH_SIZE o FLUSH_INTERVAL_MS)  ->  COPY FROM

Cada registro es una tupla lista para COPY (``received_at`` lo rellena el
DEFAULT NOW() de la BD):
    (tag_id, ts, value_num, value_str, quality, connector_id)
"""

from __future__ import annotations

import asyncio
import time
from typing import List, Tuple

import asyncpg

from ..config import DbConfig
from ..utils import metrics
from ..utils.logger import get_logger
from .spill import SpillBuffer, enqueue_or_spill

log = get_logger(__name__)

Record = Tuple[int, object, object, object, int, str]

_COLUMNS = ["tag_id", "ts", "value_num", "value_str", "quality", "connector_id"]


class BatchWriter:
    def __init__(
        self,
        pool: asyncpg.Pool,
        cfg: DbConfig,
        queue: "asyncio.Queue[Record]",
        spill: SpillBuffer | None = None,
    ) -> None:
        self._pool = pool
        self._cfg = cfg
        self._queue = queue
        self._spill = spill
        self._table = cfg.data_table
        self._stop = asyncio.Event()
        self._db_healthy = True

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        flush_s = self._cfg.flush_interval_ms / 1000.0
        batch: List[Record] = []
        last_flush = time.monotonic()

        while not self._stop.is_set():
            timeout = max(0.0, flush_s - (time.monotonic() - last_flush))
            try:
                item = await asyncio.wait_for(self._queue.get(), timeout=timeout or flush_s)
                batch.append(item)
            except asyncio.TimeoutError:
                pass

            metrics.QUEUE_SIZE.set(self._queue.qsize())

            # Reinyectar datos previamente volcados a disco cuando la BD está sana.
            if self._db_healthy and self._spill is not None and self._spill.has_data():
                replay = self._spill.drain(self._cfg.batch_size)
                if replay:
                    await self._flush(replay)

            full = len(batch) >= self._cfg.batch_size
            due = (time.monotonic() - last_flush) >= flush_s
            if batch and (full or due):
                await self._flush(batch)
                batch = []
                last_flush = time.monotonic()

        if batch:
            await self._flush(batch)

    async def _flush(self, batch: List[Record]) -> None:
        start = time.perf_counter()
        try:
            async with self._pool.acquire() as conn:
                await conn.copy_records_to_table(
                    self._table, records=batch, columns=_COLUMNS
                )
            duration = time.perf_counter() - start
            metrics.WRITE_LATENCY.observe(duration)
            metrics.VALUES_WRITTEN.inc(len(batch))
            metrics.DB_STATUS.set(1)
            self._db_healthy = True
            log.info(
                "batch_written",
                rows=len(batch),
                duration_ms=round(duration * 1000, 1),
                table=self._table,
            )
        except asyncpg.PostgresError as exc:
            metrics.DB_ERRORS.inc()
            metrics.DB_STATUS.set(0)
            self._db_healthy = False
            # No perder datos: re-encolar y, si la cola está llena, volcar a disco (spill).
            log.error("batch_write_failed", rows=len(batch), error=str(exc))
            self._requeue(batch)

    def _requeue(self, batch: List[Record]) -> None:
        for item in batch:
            enqueue_or_spill(self._queue, item, self._spill)
