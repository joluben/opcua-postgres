"""Profiling script para el conector OPC-UA → TimescaleDB.

Instrumenta los hot paths con cProfile + tracemalloc y genera un reporte
de rendimiento analysable.

Uso (dentro del contenedor o local con deps instaladas):
    python -m tools.profile_connector --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import cProfile
import io
import json
import pstats
import sys
import time
import tracemalloc
from pathlib import Path
from typing import Dict, List, Tuple

# Añadir el directorio raíz al path para importar el conector
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from connector.config import Config
from connector.db import pool as db_pool
from connector.db.initializer import initialize_schema
from connector.db.spill import SpillBuffer, enqueue_or_spill
from connector.db.writer import BatchWriter
from connector.opc import client as opc_client
from connector.opc.browser import discover_and_partition
from connector.opc.subscription import subscribe, SubHandler
from connector.utils import metrics
from connector.utils.logger import configure_logging, get_logger
from connector.utils.resilience import retry_async

log = get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Profiling del conector OPC-UA")
    p.add_argument("--duration", type=int, default=60, help="Duración del profiling en segundos")
    p.add_argument("--output", default="profile_output", help="Prefijo de ficheros de salida")
    return p.parse_args()


async def _run_with_profiling(cfg: Config, duration_s: int, output_prefix: str) -> None:
    """Ejecuta el conector con cProfile y tracemalloc activos."""
    # Iniciar tracemalloc
    tracemalloc.start()
    snapshot_before = tracemalloc.take_snapshot()

    # Iniciar cProfile
    profiler = cProfile.Profile()
    profiler.enable()

    health = metrics.HealthState()
    runner = await metrics.start_http_server(cfg.metrics_port, health)

    queue: asyncio.Queue = asyncio.Queue(maxsize=cfg.opc.queue_max_size)
    spill = SpillBuffer(cfg.db, cfg.connector_id)

    start_time = time.monotonic()

    try:
        pool = await db_pool.create_pool(cfg.db)
        health.db_connected = True
        metrics.DB_STATUS.set(1)

        await initialize_schema(pool, cfg.db)

        writer = BatchWriter(pool, cfg.db, queue, spill)
        writer_task = asyncio.create_task(writer.run(), name="batch-writer")

        client = await retry_async(
            lambda: opc_client.connect(cfg.opc),
            max_retries=cfg.reconnect_max_retries,
            base_delay_s=cfg.reconnect_base_delay_s,
            description="opc_connect",
        )
        health.opc_connected = True
        metrics.SESSION_STATUS.set(1)

        tags = await discover_and_partition(client, pool, cfg.opc, cfg.db)
        await subscribe(client, tags, queue, cfg.opc, cfg.connector_id, spill)

        log.info("profile_running", duration_s=duration_s, tags=len(tags))

        # Ejecutar durante el tiempo especificado
        elapsed = 0
        while elapsed < duration_s:
            await asyncio.sleep(5)
            elapsed = time.monotonic() - start_time
            qsize = queue.qsize()
            log.info(
                "profile_progress",
                elapsed_s=round(elapsed, 1),
                queue_size=qsize,
                values_received=metrics.VALUES_RECEIVED._value._value,
                values_written=metrics.VALUES_WRITTEN._value._value,
            )

        # Detener writer
        writer.stop()
        await writer_task

        try:
            await client.disconnect()
        except Exception:
            pass

        await pool.close()
    finally:
        health.opc_connected = False
        health.db_connected = False
        spill.close()
        await runner.cleanup()

    # Detener cProfile
    profiler.disable()

    # Snapshot de memoria
    snapshot_after = tracemalloc.take_snapshot()
    tracemalloc.stop()

    # ── Guardar resultados ──────────────────────────────────────────────────
    _save_cprofile_stats(profiler, output_prefix)
    _save_top_functions(profiler, output_prefix, top_n=30)
    _save_memory_stats(snapshot_before, snapshot_after, output_prefix)
    _save_metrics_summary(output_prefix, duration_s)

    log.info("profile_complete", output_prefix=output_prefix)


def _save_cprofile_stats(profiler: cProfile.Profile, prefix: str) -> None:
    """Guarda las stats de cProfile en formato binario."""
    path = f"{prefix}.prof"
    profiler.dump_stats(path)
    print(f"[profile] cProfile stats guardadas en {path}")


def _save_top_functions(profiler: cProfile.Profile, prefix: str, top_n: int = 30) -> None:
    """Genera un reporte de las top N funciones por tiempo acumulado y total."""
    stats = pstats.Stats(profiler)

    # Por tiempo acumulado (incluye subllamadas)
    stats.sort_stats(pstats.SortKey.CUMULATIVE)
    buf = io.StringIO()
    stats.stream = buf
    stats.print_stats(top_n)
    cumulative_output = buf.getvalue()

    # Por tiempo total (excluye subllamadas)
    stats.sort_stats(pstats.SortKey.TIME)
    buf = io.StringIO()
    stats.stream = buf
    stats.print_stats(top_n)
    totaltime_output = buf.getvalue()

    path = f"{prefix}_top_functions.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("TOP FUNCIONES POR TIEMPO ACUMULADO (cumulative)\n")
        f.write("=" * 80 + "\n\n")
        f.write(cumulative_output)
        f.write("\n" + "=" * 80 + "\n")
        f.write("TOP FUNCIONES POR TIEMPO TOTAL (tottime, excluye subllamadas)\n")
        f.write("=" * 80 + "\n\n")
        f.write(totaltime_output)
    print(f"[profile] Top funciones guardadas en {path}")


def _save_memory_stats(
    snapshot_before: tracemalloc.Snapshot,
    snapshot_after: tracemalloc.Snapshot,
    prefix: str,
) -> None:
    """Genera un reporte de uso de memoria."""
    path = f"{prefix}_memory.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write("ANÁLISIS DE MEMORIA (tracemalloc)\n")
        f.write("=" * 80 + "\n\n")

        # Top allocations por diferencia
        f.write("── Top 20 allocations por diferencia (before → after) ──\n\n")
        top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
        for stat in top_stats[:20]:
            f.write(f"  {stat}\n")

        f.write("\n\n── Top 20 allocations absolutos (snapshot final) ──\n\n")
        top_stats = snapshot_after.statistics("lineno")
        for stat in top_stats[:20]:
            f.write(f"  {stat}\n")

        # Resumen
        f.write("\n\n── Resumen ──\n\n")
        current, peak = tracemalloc.get_traced_memory()
        f.write(f"  Memoria actual traziada: {current / 1024 / 1024:.2f} MB\n")
        f.write(f"  Pico de memoria:         {peak / 1024 / 1024:.2f} MB\n")
    print(f"[profile] Análisis de memoria guardado en {path}")


def _save_metrics_summary(prefix: str, duration_s: int) -> None:
    """Guarda un resumen de las métricas Prometheus."""
    path = f"{prefix}_metrics.txt"
    with open(path, "w", encoding="utf-8") as f:
        f.write("=" * 80 + "\n")
        f.write(f"RESUMEN DE MÉTRICAS (duración: {duration_s}s)\n")
        f.write("=" * 80 + "\n\n")

        values_received = metrics.VALUES_RECEIVED._value._value
        values_written = metrics.VALUES_WRITTEN._value._value
        values_dropped = metrics.VALUES_DROPPED._value._value
        db_errors = metrics.DB_ERRORS._value._value
        spill_written = metrics.SPILL_WRITTEN._value._value
        spill_replayed = metrics.SPILL_REPLAYED._value._value

        f.write(f"  Valores recibidos (OPC-UA):  {values_received:,}\n")
        f.write(f"  Valores escritos (BD):       {values_written:,}\n")
        f.write(f"  Valores descartados:         {values_dropped:,}\n")
        f.write(f"  Throughput recibido:         {values_received / duration_s:,.0f} val/s\n")
        f.write(f"  Throughput escrito:          {values_written / duration_s:,.0f} val/s\n")
        f.write(f"  Errores de BD:               {db_errors}\n")
        f.write(f"  Spill escrito:               {spill_written:,}\n")
        f.write(f"  Spill reinyectado:           {spill_replayed:,}\n")
        f.write(f"  Pérdida:                     {values_dropped / max(values_received, 1) * 100:.3f}%\n")
    print(f"[profile] Resumen de métricas guardado en {path}")


def main() -> None:
    args = _parse_args()
    cfg = Config.from_env()
    configure_logging(cfg.log_level, cfg.log_format, cfg.connector_id)
    asyncio.run(_run_with_profiling(cfg, args.duration, args.output))


if __name__ == "__main__":
    main()
