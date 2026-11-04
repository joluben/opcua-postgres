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
import os
import signal
import time

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
    cfg: Config,
    queue: asyncio.Queue,
    health: metrics.HealthState,
    spill: SpillBuffer,
    stop_event: asyncio.Event,
) -> None:
    """Una sesión OPC-UA completa: conectar, suscribir y mantener viva hasta fallo."""
    pool = await db_pool.create_pool(cfg.db)
    health.db_connected = True
    metrics.DB_STATUS.set(1)

    # El ciclo de vida de los writers se protege desde que se crean: si la
    # conexión OPC falla tras agotar reintentos, el finally interior los para.
    writers: list[BatchWriter] = []
    writer_tasks: list[asyncio.Task] = []
    client = None
    try:
        await initialize_schema(pool, cfg.db)

        # P0: N writers COPY en paralelo (default 2). Solo worker 0 reinyecta spill.
        num_writers = max(1, min(8, int(getattr(cfg.db, "num_writers", 2))))
        writers = [
            BatchWriter(pool, cfg.db, queue, spill, worker_id=i)
            for i in range(num_writers)
        ]
        writer_tasks = [asyncio.create_task(w.run(), name=f"batch-writer-{i}")
                        for i, w in enumerate(writers)]

        try:  # el try empieza ANTES de conectar para no dejar writers huérfanos
            client = await retry_async(
                lambda: opc_client.connect(cfg.opc),
                max_retries=cfg.reconnect_max_retries,
                base_delay_s=cfg.reconnect_base_delay_s,
                description="opc_connect",
                on_retry=metrics.OPC_RECONNECTIONS.inc,
            )
            health.opc_connected = True
            metrics.SESSION_STATUS.set(1)

            tags = await discover_and_partition(client, pool, cfg.opc, cfg.db)
            await subscribe(client, tags, queue, cfg.opc, cfg.connector_id, spill)
            await _wait_until_disconnected(client, stop_event)
        finally:
            health.opc_connected = False
            metrics.SESSION_STATUS.set(0)
            if client is not None:
                try:
                    await client.disconnect()
                except Exception as exc:  # noqa: BLE001
                    log.warning("opc_disconnect_error", error=str(exc))
            await _stop_writers(
                writers, writer_tasks,
                timeout_s=getattr(cfg, "shutdown_timeout_s", 30.0),
            )
    finally:
        health.db_connected = False
        metrics.DB_STATUS.set(0)
        await pool.close()


async def _stop_writers(
    writers: list[BatchWriter],
    tasks: list[asyncio.Task],
    timeout_s: float,
) -> None:
    """Para los writers y espera su drenado final, cancelando si se agota el plazo."""
    for w in writers:
        w.stop()
    if not tasks:
        return
    done, pending = await asyncio.wait(tasks, timeout=timeout_s)
    for task in pending:
        task.cancel()
    results = await asyncio.gather(*done, *pending, return_exceptions=True)
    for result in results:
        if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
            log.warning(
                "writer_task_error",
                error=str(result),
                error_type=type(result).__name__,
            )


async def _wait_until_disconnected(client, stop_event: asyncio.Event) -> None:  # noqa: ANN001
    """Sondea la sesión OPC-UA hasta detectar desconexión o petición de parada."""
    while not stop_event.is_set():
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=5)
        except asyncio.TimeoutError:
            pass
        if stop_event.is_set():
            return
        try:
            await client.check_connection()
        except Exception as exc:  # noqa: BLE001
            log.warning("opc_session_lost", error=str(exc))
            return


async def main() -> None:
    cfg = Config.from_env()
    configure_logging(cfg.log_level, cfg.log_format, cfg.connector_id)
    # P0: versión única desde build-arg (ENV VERSION) sin divergencia con main.
    version = os.getenv("VERSION", os.getenv("CONNECTOR_VERSION", "dev"))
    log.info("connector_start", version=version, db_host=cfg.db.host, opc_url=cfg.opc.server_url)

    health = metrics.HealthState()
    runner = await metrics.start_http_server(cfg.metrics_port, health)

    # Capacidad expuesta para que las alertas calculen ocupación relativa.
    try:
        metrics.QUEUE_CAPACITY.set(cfg.opc.queue_max_size)
        metrics.DEDUP_ENABLED.set(1 if cfg.db.dedup_enabled else 0)
    except Exception:  # noqa: BLE001
        pass

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.opc.queue_max_size)
    spill = SpillBuffer(cfg.db, cfg.connector_id)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    shutdown_started: float | None = None

    def _request_stop(sig: signal.Signals) -> None:
        nonlocal shutdown_started
        if shutdown_started is None:
            shutdown_started = time.monotonic()
        log.info("shutdown_requested", signal=sig.name)
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, sig)
        except NotImplementedError:  # Windows
            pass

    try:
        attempt = 0
        while not stop_event.is_set():
            # La sesión corre en una tarea aparte para poder interrumpir también
            # las esperas no cooperativas (backoff de conexión, subscribe, etc.).
            session_task = asyncio.create_task(
                _run_opc_session(cfg, queue, health, spill, stop_event),
                name="opc-session",
            )
            stop_task = asyncio.create_task(stop_event.wait(), name="stop-wait")
            try:
                done, _ = await asyncio.wait(
                    {session_task, stop_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done and not session_task.done():
                    session_task.cancel()
            finally:
                stop_task.cancel()
                await asyncio.gather(stop_task, return_exceptions=True)

            try:
                await session_task
            except asyncio.CancelledError:
                if stop_event.is_set():
                    break
                raise
            except Exception as exc:  # noqa: BLE001
                if stop_event.is_set():
                    break
                attempt += 1
                log.error("session_failed", attempt=attempt, error=str(exc))
                if 0 <= cfg.reconnect_max_retries <= attempt:
                    log.error("max_retries_reached", action="exit_for_docker_restart")
                    break
                await asyncio.sleep(min(cfg.reconnect_base_delay_s * attempt, 30))
            else:
                attempt = 0
    finally:
        if shutdown_started is None:
            shutdown_started = time.monotonic()
        spill.close()
        await runner.cleanup()
        try:
            metrics.SHUTDOWN_SECONDS.observe(time.monotonic() - shutdown_started)
        except Exception:  # noqa: BLE001
            pass
        log.info("connector_stop")


if __name__ == "__main__":
    asyncio.run(main())
