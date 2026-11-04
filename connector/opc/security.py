"""Gestión de seguridad OPC-UA (políticas, modos y certificados X.509).

La elección se hace exclusivamente por variables de entorno (§7). Soporta:
- ``None``            (sin seguridad — solo test/red aislada, genera warning)
- ``Sign``            (firma)
- ``SignAndEncrypt``  (firma + cifrado, default fail-closed)

Políticas permitidas (P0 allow-list): Basic256Sha256, Aes128_Sha256_RsaOaep,
Aes256_Sha256_RsaPss. Se rechazan Basic128Rsa15/Basic256 por obsoletas.
"""

from __future__ import annotations

import os
from pathlib import Path

from asyncua import Client

from ..config import OpcConfig
from ..utils.logger import get_logger

log = get_logger(__name__)

_VALID_MODES = {"None", "Sign", "SignAndEncrypt"}
# P0: solo políticas modernas con SHA-256 o superior.
_VALID_POLICIES = {
    "Basic256Sha256",
    "Aes128_Sha256_RsaOaep",
    "Aes256_Sha256_RsaPss",
}


class SecurityError(RuntimeError):
    pass


def _check_file_permissions(label: str, path: str) -> None:
    """Avisa si cert/key son legibles por grupo/otros (la key debería ser 600)."""
    try:
        mode = os.stat(path).st_mode & 0o777
    except OSError:
        return
    if label == "private_key" and mode & 0o077:
        log.warning("opc_key_permissive", path=path, mode=oct(mode),
                    detail="La clave privada debería ser 600.")
    elif mode & 0o044 and label == "private_key":
        log.warning("opc_key_world_readable", path=path, mode=oct(mode))


async def apply_security(client: Client, cfg: OpcConfig) -> None:
    """Configura la seguridad del cliente OPC-UA antes de conectar."""
    if cfg.security_mode not in _VALID_MODES:
        raise SecurityError(f"OPC_SECURITY_MODE inválido: {cfg.security_mode!r}")

    if cfg.username:
        client.set_user(cfg.username)
    if cfg.password:
        client.set_password(cfg.password)

    if cfg.security_mode == "None":
        log.warning("opc_security_insecure", mode="None",
                    detail="Sin firma ni cifrado. Solo aceptable en test/red aislada.")
        return

    if cfg.security_policy not in _VALID_POLICIES:
        raise SecurityError(
            f"OPC_SECURITY_POLICY obsoleta o inválida: {cfg.security_policy!r}. "
            f"Válidas: {sorted(_VALID_POLICIES)}"
        )

    for label, path in (("certificate", cfg.certificate_path), ("private_key", cfg.private_key_path)):
        if not path or not Path(path).is_file():
            raise SecurityError(f"Fichero de {label} no encontrado: {path!r}")
        _check_file_permissions(label, path)
        # Chequeo básico de fichero vacío (cert/key corruptos).
        try:
            if Path(path).stat().st_size == 0:
                raise SecurityError(f"Fichero de {label} vacío: {path!r}")
        except OSError as exc:
            raise SecurityError(f"No se puede leer {label}: {path!r}: {exc}") from exc

    # Formato: "Policy,Mode,cert_path,key_path"
    security_string = (
        f"{cfg.security_policy},{cfg.security_mode},"
        f"{cfg.certificate_path},{cfg.private_key_path}"
    )
    await client.set_security_string(security_string)
    log.info("opc_security", mode=cfg.security_mode, policy=cfg.security_policy)
