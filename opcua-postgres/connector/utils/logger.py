"""Configuración de logging estructurado con structlog.

- ``LOG_FORMAT=json`` (producción): salida JSON, integrable con ELK/Loki.
- ``LOG_FORMAT=pretty`` (desarrollo): salida coloreada legible.

Regla de seguridad: nunca loggear credenciales ni valores de proceso completos.
"""

from __future__ import annotations

import logging
import os
import sys

import structlog


def configure_logging(level: str = "INFO", fmt: str = "json", connector_id: str = "") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)

    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=log_level)

    # Las librerías OPC-UA (asyncua) emiten cada DataValue/notificación a nivel INFO,
    # lo que satura los logs y consume CPU bajo carga. Se limitan por defecto a WARNING,
    # configurable con OPC_LIB_LOG_LEVEL para depuración puntual.
    lib_level = getattr(logging, os.getenv("OPC_LIB_LOG_LEVEL", "WARNING").upper(), logging.WARNING)
    for lib in ("asyncua", "opcua"):
        logging.getLogger(lib).setLevel(lib_level)

    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if fmt == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    if connector_id:
        structlog.contextvars.bind_contextvars(connector_id=connector_id)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)
