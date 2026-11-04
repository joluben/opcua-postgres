"""Cliente de carga / *spike* de throughput de `asyncua` (Fase 5, §14).

Se suscribe a todas las variables de la carpeta del servidor simulado y mide, SIN tocar la
base de datos, cuántas notificaciones DataChange por segundo es capaz de deserializar `asyncua`
en Python, junto con la latencia (SourceTimestamp → recepción). Este es el dato clave para
dimensionar el particionamiento (nº de conectores).

Uso:
    python tools/load_client.py --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
from collections import deque
from datetime import datetime, timezone

from asyncua import Client, ua

_UTC = timezone.utc


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spike de throughput de asyncua")
    p.add_argument("--endpoint", default="opc.tcp://localhost:4840/freeopcua/server/")
    p.add_argument("--namespace-uri", default="http://sim.opcua/connector")
    p.add_argument("--folder", default="SimTags")
    p.add_argument("--publish-interval-ms", type=int, default=200)
    p.add_argument("--queue-size", type=int, default=10, help="QueueSize del MonitoredItem en el servidor")
    p.add_argument("--max-tags", type=int, default=0, help="Limitar nº de tags suscritos (0 = todos)")
    p.add_argument("--report-interval", type=int, default=5, help="Segundos entre reportes")
    p.add_argument("--duration", type=int, default=0, help="Duración total en s (0 = infinito)")
    return p.parse_args()


class CountingHandler:
    """Cuenta notificaciones y muestrea latencias (cola acotada)."""

    def __init__(self) -> None:
        self.count = 0
        self.total = 0
        self._latencies: deque[float] = deque(maxlen=100_000)

    def datachange_notification(self, node, val, data) -> None:  # noqa: ANN001
        self.count += 1
        self.total += 1
        dv = data.monitored_item.Value
        ts = getattr(dv, "SourceTimestamp", None)
        if ts is not None:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=_UTC)
            self._latencies.append((datetime.now(_UTC) - ts).total_seconds() * 1000.0)

    def snapshot(self) -> tuple[int, list[float]]:
        c, self.count = self.count, 0
        return c, list(self._latencies)


async def _resolve_nodes(client: Client, args: argparse.Namespace):
    idx = await client.get_namespace_index(args.namespace_uri)
    folder = await client.nodes.objects.get_child(f"{idx}:{args.folder}")
    nodes = await folder.get_children()
    if args.max_tags > 0:
        nodes = nodes[: args.max_tags]
    return nodes


def _percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = min(len(s) - 1, int(round((pct / 100.0) * (len(s) - 1))))
    return s[k]


async def main() -> None:
    args = _parse_args()
    handler = CountingHandler()

    client = Client(url=args.endpoint)
    await client.connect()
    print(f"[load] conectado a {args.endpoint}")
    try:
        nodes = await _resolve_nodes(client, args)
        print(f"[load] suscribiendo {len(nodes)} tags (publish={args.publish_interval_ms}ms)...")

        subscription = await client.create_subscription(args.publish_interval_ms, handler)
        # Suscribir en bloques para no saturar una sola petición.
        for start in range(0, len(nodes), 1000):
            await subscription.subscribe_data_change(
                nodes[start : start + 1000], queuesize=args.queue_size
            )
        print("[load] suscripción activa. Midiendo throughput...\n")

        start_time = time.monotonic()
        last = start_time
        peak = 0.0
        while True:
            await asyncio.sleep(args.report_interval)
            now = time.monotonic()
            elapsed = now - last
            last = now

            count, lats = handler.snapshot()
            rate = count / elapsed if elapsed else 0.0
            peak = max(peak, rate)
            p50 = _percentile(lats, 50)
            p95 = _percentile(lats, 95)
            mean_lat = statistics.fmean(lats) if lats else 0.0
            print(
                f"[load] {rate:>10,.0f} val/s | "
                f"lat ms p50={p50:6.1f} p95={p95:6.1f} avg={mean_lat:6.1f} | "
                f"total={handler.total:,}"
            )

            if args.duration and (now - start_time) >= args.duration:
                break

        print(f"\n[load] RESUMEN: pico={peak:,.0f} val/s, total={handler.total:,} en {now - start_time:.0f}s")
    finally:
        await client.disconnect()
        print("[load] desconectado")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[load] detenido")
