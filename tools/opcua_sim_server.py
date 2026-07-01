"""Servidor OPC-UA simulado para pruebas de carga (Fase 5).

Expone N variables bajo una carpeta y genera cambios de valor a una tasa controlada,
produciendo notificaciones DataChange para validar el throughput del conector / `asyncua`.

Uso:
    python tools/opcua_sim_server.py --tags 10000 --changes-per-sec 20000

El control de carga es por **cambios por segundo** (independiente del nº de tags): en cada
ciclo (`--cycle-ms`) se escribe un subconjunto de tags en round-robin hasta alcanzar la tasa.
Seguridad: ninguna (modo `None`) para simplificar las pruebas locales.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import random
import time

from asyncua import Server, ua


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Servidor OPC-UA simulado")
    p.add_argument("--endpoint", default="opc.tcp://0.0.0.0:4840/freeopcua/server/")
    p.add_argument("--namespace-uri", default="http://sim.opcua/connector")
    p.add_argument("--tags", type=int, default=1000, help="Número de variables a exponer")
    p.add_argument("--changes-per-sec", type=int, default=5000, help="Tasa objetivo de cambios/s (0 = todos cada ciclo)")
    p.add_argument("--cycle-ms", type=int, default=100, help="Periodo del bucle de actualización")
    p.add_argument("--folder", default="SimTags")
    return p.parse_args()


def _value(i: int, t: float) -> float:
    """Onda senoidal por tag + ruido, para generar cambios realistas."""
    freq = 0.05 + (i % 50) * 0.01
    phase = (i % 360) * math.pi / 180.0
    return round(50.0 + 40.0 * math.sin(2 * math.pi * freq * t + phase) + random.uniform(-0.5, 0.5), 4)


async def main() -> None:
    args = _parse_args()

    server = Server()
    await server.init()
    server.set_endpoint(args.endpoint)
    server.set_server_name("Simulated OPC-UA Server")
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])

    idx = await server.register_namespace(args.namespace_uri)
    folder = await server.nodes.objects.add_folder(idx, args.folder)

    print(f"[sim] creando {args.tags} variables bajo {args.folder} (ns={idx})...")
    nodes = []
    for i in range(args.tags):
        var = await folder.add_variable(idx, f"Tag_{i:05d}", 0.0)
        nodes.append(var)
    print(f"[sim] endpoint: {args.endpoint}  ns_uri: {args.namespace_uri}")

    cycle_s = args.cycle_ms / 1000.0
    if args.changes_per_sec <= 0:
        per_cycle = len(nodes)
    else:
        per_cycle = max(1, int(args.changes_per_sec * cycle_s))

    print(f"[sim] generando ~{args.changes_per_sec} cambios/s ({per_cycle} writes/ciclo de {args.cycle_ms}ms)")

    async with server:
        cursor = 0
        last_report = time.monotonic()
        writes = 0
        while True:
            t = time.time()
            for _ in range(per_cycle):
                node = nodes[cursor % len(nodes)]
                cursor += 1
                await node.write_value(_value(cursor, t))
                writes += 1

            now = time.monotonic()
            if now - last_report >= 5.0:
                rate = writes / (now - last_report)
                print(f"[sim] writes/s reales: {rate:,.0f}")
                writes = 0
                last_report = now

            await asyncio.sleep(cycle_s)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[sim] detenido")
