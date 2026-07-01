"""Gestión de seguridad OPC-UA (políticas, modos y certificados X.509).

La elección se hace exclusivamente por variables de entorno (§7). Soporta:
- ``None``            (sin seguridad)
- ``Sign``            (firma)
- ``SignAndEncrypt``  (firma + cifrado)
"""

from __future__ import annotations

from pathlib import Path

from asyncua import Client

from ..config import OpcConfig
from ..utils.logger import get_logger

log = get_logger(__name__)

_VALID_MODES = {"None", "Sign", "SignAndEncrypt"}


class SecurityError(RuntimeError):
    pass


async def apply_security(client: Client, cfg: OpcConfig) -> None:
    """Configura la seguridad del cliente OPC-UA antes de conectar."""
    if cfg.security_mode not in _VALID_MODES:
        raise SecurityError(f"OPC_SECURITY_MODE inválido: {cfg.security_mode!r}")

    if cfg.username:
        client.set_user(cfg.username)
    if cfg.password:
        client.set_password(cfg.password)

    if cfg.security_mode == "None":
        log.info("opc_security", mode="None", policy="None")
        return

    for label, path in (("certificate", cfg.certificate_path), ("private_key", cfg.private_key_path)):
        if not path or not Path(path).is_file():
            raise SecurityError(f"Fichero de {label} no encontrado: {path!r}")

    # Formato: "Policy,Mode,cert_path,key_path"
    security_string = (
        f"{cfg.security_policy},{cfg.security_mode},"
        f"{cfg.certificate_path},{cfg.private_key_path}"
    )
    await client.set_security_string(security_string)
    log.info("opc_security", mode=cfg.security_mode, policy=cfg.security_policy)
