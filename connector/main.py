"""Punto de entrada del conector OPC-UA → TimescaleDB.

Orquesta:
  1. Carga de configuración y logging.
  2. Servidor HTTP de observabilidad (/metrics, /health).
  3. Pool asyncpg + inicialización idempotente del schema (BD remota).
  4. Conexión OPC-UA (con backoff), descubrimiento/partición y suscripción.
  5. BatchWriter consumiendo la cola e insertando con COPY.
  6. Reconexión OPC-UA y apagado ordenado por señal.
"""

from __future__ import annotations

import asyncio
import signal

from .config import Config
from .db import pool as db_pool
from .db.initializer import initialize_schema
from .db.spill import SpillBuffer
from .db.writer import BatchWriter
from .opc import client as opc_client
from .opc.browser import discover_and_partition
from .opc.subscription import subscribe
from .utils import metrics
from .utils.logger import configure_logging, get_logger
from .utils.resilience import retry_async

log = get_logger(__name__)


async def _run_opc_session(
    cfg: Config, queue: asyncio.Queue, health: metrics.HealthState, spill: SpillBuffer
) -> None:
    """Una sesión OPC-UA completa: conectar, suscribir y mantener viva hasta fallo."""
    pool = await db_pool.create_pool(cfg.db)
    health.db_connected = True
    metrics.DB_STATUS.set(1)
    try:
        await initialize_schema(pool, cfg.db)

        writer = BatchWriter(pool, cfg.db, queue, spill)
        writer_task = asyncio.create_task(writer.run(), name="batch-writer")

        client = await retry_async(
            lambda: opc_client.connect(cfg.opc),
            max_retries=cfg.reconnect_max_retries,
            base_delay_s=cfg.reconnect_base_delay_s,
            description="opc_connect",
            on_retry=metrics.OPC_RECONNECTIONS.inc,
        )
        health.opc_connected = True
        metrics.SESSION_STATUS.set(1)

        try:
            tags = await discover_and_partition(client, pool, cfg.opc, cfg.db)
            await subscribe(client, tags, queue, cfg.opc, cfg.connector_id, spill)
            await _wait_until_disconnected(client)
        finally:
            health.opc_connected = False
            metrics.SESSION_STATUS.set(0)
            try:
                await client.disconnect()
            except Exception as exc:  # noqa: BLE001
                log.warning("opc_disconnect_error", error=str(exc))
            writer.stop()
            await writer_task
    finally:
        await pool.close()
        health.db_connected = False
        metrics.DB_STATUS.set(0)


async def _wait_until_disconnected(client) -> None:  # noqa: ANN001
    """Sondea el estado de la sesión OPC-UA hasta detectar desconexión."""
    while True:
        await asyncio.sleep(5)
        try:
            await client.check_connection()
        except Exception as exc:  # noqa: BLE001
            log.warning("opc_session_lost", error=str(exc))
            return


async def main() -> None:
    cfg = Config.from_env()
    configure_logging(cfg.log_level, cfg.log_format, cfg.connector_id)
    log.info("connector_start", version="1.1.0", db_host=cfg.db.host, opc_url=cfg.opc.server_url)

    health = metrics.HealthState()
    runner = await metrics.start_http_server(cfg.metrics_port, health)

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.opc.queue_max_size)
    spill = SpillBuffer(cfg.db, cfg.connector_id)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:  # Windows
            pass

    try:
        attempt = 0
        while not stop_event.is_set():
            try:
                await _run_opc_session(cfg, queue, health, spill)
            except Exception as exc:  # noqa: BLE001
                attempt += 1
                log.error("session_failed", attempt=attempt, error=str(exc))
                if 0 <= cfg.reconnect_max_retries <= attempt:
                    log.error("max_retries_reached", action="exit_for_docker_restart")
                    break
                await asyncio.sleep(min(cfg.reconnect_base_delay_s * attempt, 30))
            else:
                attempt = 0
    finally:
        spill.close()
        await runner.cleanup()
        log.info("connector_stop")


if __name__ == "__main__":
    asyncio.run(main())
