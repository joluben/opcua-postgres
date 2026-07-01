"""Cliente OPC-UA y gestión de sesión."""

from __future__ import annotations

from asyncua import Client

from ..config import OpcConfig
from ..utils.logger import get_logger
from .security import apply_security

log = get_logger(__name__)


async def connect(cfg: OpcConfig) -> Client:
    """Crea y conecta un cliente OPC-UA con la seguridad configurada.

    El llamador es responsable de cerrar la sesión con ``await client.disconnect()``.
    """
    client = Client(url=cfg.server_url, timeout=cfg.session_timeout_ms / 1000.0)
    client.session_timeout = cfg.session_timeout_ms
    await apply_security(client, cfg)

    log.info("opc_connecting", url=cfg.server_url)
    await client.connect()
    log.info("opc_connected", url=cfg.server_url)
    return client
