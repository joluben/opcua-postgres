"""Pool de conexiones asyncpg hacia la base de datos remota.

Notas:
- La BD vive en un servidor independiente: ``host`` apunta a un destino remoto.
- ``statement_cache_size=0`` es obligatorio si se conecta a través de pgBouncer
  en modo ``transaction`` (incompatibilidad con prepared statements de asyncpg).
"""

from __future__ import annotations

import ssl as ssl_module
from typing import Optional

import asyncpg

from ..config import DbConfig
from ..utils.logger import get_logger

log = get_logger(__name__)


def _build_ssl(ssl_mode: str) -> Optional[ssl_module.SSLContext | bool]:
    """Traduce ``POSTGRES_SSL_MODE`` a un parámetro ssl válido para asyncpg.

    Modos soportados (equivalente a libpq):
    - ``disable``     → sin TLS.
    - ``allow``/``prefer``  → TLS sin verificar certificado ni hostname.
    - ``require``     → TLS obligatorio; no verifica CA ni hostname (cifrado sin autenticación).
    - ``verify-ca``   → TLS + verifica CA del servidor; no verifica hostname.
    - ``verify-full`` → TLS + verifica CA + verifica hostname (máxima seguridad, recomendado).

    Para ``verify-ca`` y ``verify-full`` el store de CA del sistema debe incluir la CA
    del servidor de BD, o montar ``certs/ca.pem`` y configurar ``PGSSLROOTCERT``.
    """
    mode = (ssl_mode or "prefer").lower()
    if mode == "disable":
        return False
    if mode in ("allow", "prefer"):
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl_module.CERT_NONE
        return ctx
    if mode == "require":
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl_module.CERT_NONE
        return ctx
    if mode == "verify-ca":
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl_module.CERT_REQUIRED
        return ctx
    if mode == "verify-full":
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl_module.CERT_REQUIRED
        return ctx
    log.warning("ssl_mode_unknown", ssl_mode=ssl_mode, fallback="require")
    ctx = ssl_module.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl_module.CERT_NONE
    return ctx


async def create_pool(cfg: DbConfig) -> asyncpg.Pool:
    ssl_param = _build_ssl(cfg.ssl_mode)
    log.info(
        "db_pool_create",
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        ssl_mode=cfg.ssl_mode,
        pool_min=cfg.pool_min,
        pool_max=cfg.pool_max,
    )
    return await asyncpg.create_pool(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        ssl=ssl_param,
        min_size=cfg.pool_min,
        max_size=cfg.pool_max,
        statement_cache_size=cfg.statement_cache_size,
        command_timeout=60,
    )
