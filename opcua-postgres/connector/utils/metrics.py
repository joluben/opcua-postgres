"""Métricas Prometheus y servidor HTTP (/metrics y /health).

Expone un pequeño servidor aiohttp que sirve:
- ``GET /metrics``  → formato de exposición Prometheus.
- ``GET /health``   → 200 si OPC y BD están conectados, 503 si degradado.
"""

from __future__ import annotations

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

# ── Definición de métricas (§13.1) ───────────────────────────────────────────
TAGS_TOTAL = Gauge("opc_connector_tags_total", "Total de tags suscritos")
VALUES_RECEIVED = Counter("opc_connector_values_received_total", "Valores recibidos del servidor OPC-UA")
VALUES_WRITTEN = Counter("opc_connector_values_written_total", "Valores escritos en TimescaleDB")
VALUES_DROPPED = Counter("opc_connector_values_dropped_total", "Valores descartados por buffer lleno")
QUEUE_SIZE = Gauge("opc_connector_queue_size", "Tamaño actual del buffer en memoria")
WRITE_LATENCY = Histogram("opc_connector_write_latency_seconds", "Latencia de escritura por lote en BD")
DB_ERRORS = Counter("opc_connector_db_errors_total", "Errores de escritura en BD")
OPC_RECONNECTIONS = Counter("opc_connector_opc_reconnections_total", "Reconexiones al servidor OPC-UA")
SESSION_STATUS = Gauge("opc_connector_session_status", "Estado sesión OPC-UA (1=ok, 0=ko)")
DB_STATUS = Gauge("opc_connector_db_status", "Estado conexión BD (1=ok, 0=ko)")

# Spill a disco (buffer persistente ante caídas largas de BD)
SPILL_WRITTEN = Counter("opc_connector_spill_written_total", "Registros volcados a disco (spill)")
SPILL_REPLAYED = Counter("opc_connector_spill_replayed_total", "Registros releídos desde disco")
SPILL_DROPPED = Counter("opc_connector_spill_dropped_total", "Segmentos de spill descartados por límite de disco")
SPILL_BYTES = Gauge("opc_connector_spill_bytes", "Bytes actuales en el buffer de spill en disco")
SPILL_FILES = Gauge("opc_connector_spill_files", "Número de segmentos de spill en disco")


class HealthState:
    """Estado compartido para el endpoint /health."""

    def __init__(self) -> None:
        self.opc_connected = False
        self.db_connected = False


async def _metrics_handler(_request: web.Request) -> web.Response:
    return web.Response(body=generate_latest(), content_type=CONTENT_TYPE_LATEST.split(";")[0])


def _make_health_handler(state: HealthState):
    async def _handler(_request: web.Request) -> web.Response:
        healthy = state.opc_connected and state.db_connected
        payload = {
            "status": "healthy" if healthy else "degraded",
            "opc_connected": state.opc_connected,
            "db_connected": state.db_connected,
        }
        return web.json_response(payload, status=200 if healthy else 503)

    return _handler


async def start_http_server(port: int, state: HealthState) -> web.AppRunner:
    """Arranca el servidor HTTP de observabilidad y devuelve el runner para cerrarlo."""
    app = web.Application()
    app.router.add_get("/metrics", _metrics_handler)
    app.router.add_get("/health", _make_health_handler(state))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    return runner
