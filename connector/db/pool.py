"""Pool de conexiones asyncpg hacia la base de datos remota.

Notas:
- La BD vive en un servidor independiente: ``host`` apunta a un destino remoto.
- ``statement_cache_size=0`` es obligatorio si se conecta a través de pgBouncer
  en modo ``transaction`` (incompatibilidad con prepared statements de asyncpg).
"""

from __future__ import annotations

import os
import ssl as ssl_module
from pathlib import Path
from typing import Optional

import asyncpg

from ..config import DbConfig
from ..utils.logger import get_logger

log = get_logger(__name__)


def _load_root_ca(ctx: ssl_module.SSLContext) -> None:
    """Carga la CA del servidor de BD si se indica ``PGSSLROOTCERT``.

    Para ``verify-ca``/``verify-full`` el store del sistema debe incluir la CA del
    servidor, o bien se monta ``certs/ca.pem`` y se exporta ``PGSSLROOTCERT``
    (equivalente a libpq). Si la variable apunta a un fichero inexistente se
    falla en cerrado (fail-closed) para no degradar silenciosamente la verificación.
    """
    ca = os.getenv("PGSSLROOTCERT") or os.getenv("SSLROOTCERT")
    if not ca:
        return
    if not Path(ca).is_file():
        raise RuntimeError(
            f"PGSSLROOTCERT apunta a un fichero inexistente: {ca!r}. "
            "Monte la CA o corrija la variable."
        )
    ctx.load_verify_locations(cafile=ca)
    log.info("ssl_root_ca_loaded", path=ca)


def _build_ssl(ssl_mode: str) -> Optional[ssl_module.SSLContext | bool]:
    """Traduce ``POSTGRES_SSL_MODE`` a un parámetro ssl válido para asyncpg.

    Modos soportados (equivalente a libpq):
    - ``disable``     → sin TLS (solo test local; warning crítico).
    - ``allow``/``prefer``  → TLS sin verificar certificado ni hostname (MITM posible).
    - ``require``     → TLS obligatorio; no verifica CA ni hostname (cifrado sin autenticación).
    - ``verify-ca``   → TLS + verifica CA del servidor; no verifica hostname.
    - ``verify-full`` → TLS + verifica CA + verifica hostname (máxima seguridad, recomendado).

    Para ``verify-ca`` y ``verify-full`` el store de CA del sistema debe incluir la CA
    del servidor de BD, o montar ``certs/ca.pem`` y configurar ``PGSSLROOTCERT``.

    P0: los modos sin verificación emiten warning; un modo desconocido falla
    a modo seguro (verify-full) en lugar de degradar a CERT_NONE.
    """
    mode = (ssl_mode or "verify-full").lower()
    if mode == "disable":
        log.warning("ssl_insecure", ssl_mode=mode,
                    detail="TLS deshabilitado. Solo aceptable en test local aislado.")
        return False
    if mode in ("allow", "prefer"):
        log.warning("ssl_insecure", ssl_mode=mode,
                    detail="No verifica certificado ni hostname (MITM posible). Use verify-full.")
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl_module.CERT_NONE
        return ctx
    if mode == "require":
        log.warning("ssl_insecure", ssl_mode=mode,
                    detail="Cifra sin autenticar el servidor (MITM posible). Use verify-full.")
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl_module.CERT_NONE
        return ctx
    if mode == "verify-ca":
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl_module.CERT_REQUIRED
        _load_root_ca(ctx)
        return ctx
    if mode == "verify-full":
        ctx = ssl_module.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl_module.CERT_REQUIRED
        _load_root_ca(ctx)
        return ctx
    # P0 fail-closed: modo desconocido → no degradar a CERT_NONE.
    log.warning("ssl_mode_unknown", ssl_mode=ssl_mode, fallback="verify-full")
    ctx = ssl_module.create_default_context()
    ctx.check_hostname = True
    ctx.verify_mode = ssl_module.CERT_REQUIRED
    return ctx


async def create_pool(cfg: DbConfig) -> asyncpg.Pool:
    ssl_param = _build_ssl(cfg.ssl_mode)
    # P0: timeout de comando acotado (antes 60s bloqueaba el flush).
    command_timeout = getattr(cfg, "command_timeout_s", 15.0)
    log.info(
        "db_pool_create",
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        ssl_mode=cfg.ssl_mode,
        pool_min=cfg.pool_min,
        pool_max=cfg.pool_max,
        command_timeout_s=command_timeout,
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
        command_timeout=command_timeout,
        timeout=10.0,
    )
