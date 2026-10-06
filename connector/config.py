"""Carga y validación de la configuración desde variables de entorno.

Reglas (P0 fail-closed):
- Ninguna credencial se hardcodea.
- Soporta la convención Docker Secrets ``<VAR>_FILE`` (p.ej. ``POSTGRES_PASSWORD_FILE``):
  si existe el fichero, su contenido tiene prioridad sobre la variable en claro.
- Defaults seguros: OPC_SECURITY_MODE=SignAndEncrypt, POSTGRES_SSL_MODE=verify-full.
  Los modos inseguros (None/disable/prefer/allow/require) solo se aceptan de forma
  explícita y generan warning.
"""

from __future__ import annotations

import os
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class ConfigError(RuntimeError):
    """Error de configuración (variable obligatoria ausente o inválida)."""


# ── Allow-lists P0 ──────────────────────────────────────────────────────────
VALID_SECURITY_MODES = {"None", "Sign", "SignAndEncrypt"}
# Solo políticas modernas. Se rechazan Basic128Rsa15 y Basic256 (obsoletas).
VALID_SECURITY_POLICIES = {
    "Basic256Sha256",
    "Aes128_Sha256_RsaOaep",
    "Aes256_Sha256_RsaPss",
}
VALID_DEADBAND_TYPES = {"None", "Absolute", "Percent"}
VALID_SSL_MODES = {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}
INSECURE_SSL_MODES = {"disable", "allow", "prefer", "require"}


def _read_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """Lee ``<name>`` priorizando el fichero indicado por ``<name>_FILE``.

    Fail-closed: si ``<name>_FILE`` está definido pero el fichero no existe,
    se emite un warning (evita enmascarar un secret mal montado) y se cae
    a la variable en claro / default.
    """
    file_path = os.getenv(f"{name}_FILE")
    if file_path:
        p = Path(file_path)
        if p.is_file():
            # Docker Secrets se montan siempre como 0444 (solo lectura, owner root):
            # avisar de "legible por grupo/otros" ahí no es accionable y ensucia el
            # arranque. Solo se comprueban permisos fuera de /run/secrets.
            is_docker_secret = str(p).replace("\\", "/").startswith("/run/secrets/")
            if not is_docker_secret:
                try:
                    mode = p.stat().st_mode & 0o777
                    if mode & 0o044:
                        warnings.warn(
                            f"{name}_FILE ({file_path}) es legible por grupo/otros "
                            f"(mode {oct(mode)}). Use chmod 600.",
                            UserWarning,
                            stacklevel=2,
                        )
                except OSError:
                    pass
            content = p.read_text(encoding="utf-8").strip()
            # Fichero vacío (p.ej. opc_password sin auth) → tratar como ausente.
            return content if content else default
        warnings.warn(
            f"{name}_FILE apunta a fichero inexistente: {file_path!r}. "
            f"Cayendo a la variable {name} en claro.",
            UserWarning,
            stacklevel=2,
        )
    return os.getenv(name, default)


def _required(name: str) -> str:
    value = _read_secret(name)
    if value is None or value == "":
        raise ConfigError(f"Variable de entorno obligatoria ausente: {name}")
    return value


def _int(name: str, default: int, *, min_value: int | None = None,
         max_value: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:  # noqa: BLE001
            raise ConfigError(f"{name} debe ser entero, recibido: {raw!r}") from exc
    if min_value is not None and value < min_value:
        raise ConfigError(f"{name} debe ser >= {min_value}, recibido: {value}")
    if max_value is not None and value > max_value:
        raise ConfigError(f"{name} debe ser <= {max_value}, recibido: {value}")
    return value


def _float(name: str, default: float, *, min_value: float | None = None,
           max_value: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        value = default
    else:
        try:
            value = float(raw)
        except ValueError as exc:  # noqa: BLE001
            raise ConfigError(f"{name} debe ser numérico, recibido: {raw!r}") from exc
    if min_value is not None and value < min_value:
        raise ConfigError(f"{name} debe ser >= {min_value}, recibido: {value}")
    if max_value is not None and value > max_value:
        raise ConfigError(f"{name} debe ser <= {max_value}, recibido: {value}")
    return value


def _opt(name: str) -> Optional[str]:
    value = os.getenv(name)
    return value if value else None


def _bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class OpcConfig:
    server_url: str
    username: Optional[str]
    password: Optional[str]
    security_policy: str
    security_mode: str
    certificate_path: Optional[str]
    private_key_path: Optional[str]
    publish_interval_ms: int
    deadband: float
    deadband_type: str
    namespace_index: Optional[int]
    node_id_filter: Optional[str]
    tag_offset: int
    tag_limit: int
    session_timeout_ms: int
    queue_max_size: int


@dataclass(frozen=True)
class DbConfig:
    host: str
    port: int
    database: str
    user: str
    password: str
    ssl_mode: str
    catalog_table: str
    data_table: str
    batch_size: int
    flush_interval_ms: int
    pool_min: int
    pool_max: int
    statement_cache_size: int
    command_timeout_s: float
    num_writers: int
    use_timescale: bool
    spill_enabled: bool
    spill_dir: str
    spill_max_mb: int
    spill_segment_mb: int
    spill_fsync_every: int


@dataclass(frozen=True)
class Config:
    connector_id: str
    log_level: str
    log_format: str
    metrics_port: int
    reconnect_max_retries: int
    reconnect_base_delay_s: float
    opc: OpcConfig
    db: DbConfig

    @classmethod
    def from_env(cls) -> "Config":
        # P0 fail-closed: por defecto SignAndEncrypt + verify-full.
        security_mode = os.getenv("OPC_SECURITY_MODE", "SignAndEncrypt")
        if security_mode not in VALID_SECURITY_MODES:
            raise ConfigError(
                f"OPC_SECURITY_MODE inválido: {security_mode!r}. "
                f"Válidos: {sorted(VALID_SECURITY_MODES)}"
            )
        security_policy = os.getenv("OPC_SECURITY_POLICY", "Basic256Sha256")
        if security_mode != "None" and security_policy not in VALID_SECURITY_POLICIES:
            raise ConfigError(
                f"OPC_SECURITY_POLICY obsoleta o inválida: {security_policy!r}. "
                f"Válidas: {sorted(VALID_SECURITY_POLICIES)}"
            )
        if security_mode == "None":
            warnings.warn(
                "OPC_SECURITY_MODE=None: tráfico OPC-UA sin firma ni cifrado. "
                "Solo aceptable en red aislada/test.",
                UserWarning,
                stacklevel=2,
            )
        cert_path = _opt("OPC_CERTIFICATE_PATH")
        key_path = _opt("OPC_PRIVATE_KEY_PATH")

        if security_mode != "None" and (not cert_path or not key_path):
            raise ConfigError(
                "OPC_CERTIFICATE_PATH y OPC_PRIVATE_KEY_PATH son obligatorios "
                f"cuando OPC_SECURITY_MODE={security_mode!r}"
            )

        deadband_type = os.getenv("OPC_DEADBAND_TYPE", "None")
        if deadband_type not in VALID_DEADBAND_TYPES:
            raise ConfigError(
                f"OPC_DEADBAND_TYPE inválido: {deadband_type!r}. "
                f"Válidos: {sorted(VALID_DEADBAND_TYPES)}"
            )

        ns_raw = os.getenv("OPC_NAMESPACE_INDEX")
        try:
            namespace_index = int(ns_raw) if ns_raw else None
        except ValueError as exc:
            raise ConfigError(
                f"OPC_NAMESPACE_INDEX debe ser entero, recibido: {ns_raw!r}"
            ) from exc
        if namespace_index is not None and namespace_index < 0:
            raise ConfigError("OPC_NAMESPACE_INDEX debe ser >= 0")

        opc = OpcConfig(
            server_url=_required("OPC_SERVER_URL"),
            username=_opt("OPC_USERNAME"),
            password=_read_secret("OPC_PASSWORD"),
            security_policy=security_policy,
            security_mode=security_mode,
            certificate_path=cert_path,
            private_key_path=key_path,
            publish_interval_ms=_int("OPC_PUBLISH_INTERVAL_MS", 500, min_value=50),
            deadband=_float("OPC_DATACHANGE_DEADBAND", 0.0, min_value=0.0),
            deadband_type=deadband_type,
            namespace_index=namespace_index,
            node_id_filter=_opt("OPC_NODE_ID_FILTER"),
            tag_offset=_int("OPC_TAG_OFFSET", 0, min_value=0),
            tag_limit=_int("OPC_TAG_LIMIT", 5000, min_value=1, max_value=100000),
            session_timeout_ms=_int("OPC_SESSION_TIMEOUT_MS", 30000, min_value=5000),
            queue_max_size=_int("OPC_QUEUE_MAX_SIZE", 500000, min_value=1000),
        )

        ssl_mode = os.getenv("POSTGRES_SSL_MODE", "verify-full")
        if ssl_mode not in VALID_SSL_MODES:
            raise ConfigError(
                f"POSTGRES_SSL_MODE inválido: {ssl_mode!r}. "
                f"Válidos: {sorted(VALID_SSL_MODES)}"
            )
        if ssl_mode in INSECURE_SSL_MODES:
            warnings.warn(
                f"POSTGRES_SSL_MODE={ssl_mode!r} no verifica el certificado del "
                "servidor (vulnerable a MITM). Use verify-full en producción.",
                UserWarning,
                stacklevel=2,
            )

        pool_min = _int("POSTGRES_POOL_MIN", 2, min_value=1, max_value=50)
        pool_max = _int("POSTGRES_POOL_MAX", 10, min_value=1, max_value=50)
        if pool_max < pool_min:
            raise ConfigError(
                f"POSTGRES_POOL_MAX ({pool_max}) debe ser >= POSTGRES_POOL_MIN ({pool_min})"
            )

        db = DbConfig(
            host=_required("POSTGRES_HOST"),
            port=_int("POSTGRES_PORT", 5432, min_value=1, max_value=65535),
            database=_required("POSTGRES_DB"),
            user=_required("POSTGRES_USER"),
            password=_required("POSTGRES_PASSWORD"),
            ssl_mode=ssl_mode,
            catalog_table=os.getenv("POSTGRES_CATALOG_TABLE", "opc_tags_catalog"),
            data_table=os.getenv("POSTGRES_DATA_TABLE", "opc_raw_values"),
            batch_size=_int("POSTGRES_BATCH_SIZE", 1000, min_value=10, max_value=50000),
            flush_interval_ms=_int("POSTGRES_FLUSH_INTERVAL_MS", 500, min_value=50),
            pool_min=pool_min,
            pool_max=pool_max,
            statement_cache_size=_int("POSTGRES_STATEMENT_CACHE_SIZE", 100, min_value=0),
            command_timeout_s=_float("POSTGRES_COMMAND_TIMEOUT_S", 15.0,
                                     min_value=2.0, max_value=120.0),
            num_writers=_int("POSTGRES_NUM_WRITERS", 2, min_value=1, max_value=8),
            use_timescale=_bool("POSTGRES_USE_TIMESCALE", True),
            spill_enabled=_bool("POSTGRES_SPILL_ENABLED", True),
            spill_dir=os.getenv("POSTGRES_SPILL_DIR", "/var/lib/connector/spill"),
            spill_max_mb=_int("POSTGRES_SPILL_MAX_MB", 1024, min_value=1),
            spill_segment_mb=_int("POSTGRES_SPILL_SEGMENT_MB", 64, min_value=1),
            spill_fsync_every=_int("POSTGRES_SPILL_FSYNC_EVERY", 100, min_value=1),
        )

        return cls(
            connector_id=_required("CONNECTOR_ID"),
            log_level=os.getenv("LOG_LEVEL", "INFO"),
            log_format=os.getenv("LOG_FORMAT", "json"),
            metrics_port=_int("METRICS_PORT", 8000),
            reconnect_max_retries=_int("RECONNECT_MAX_RETRIES", 10),
            reconnect_base_delay_s=_float("RECONNECT_BASE_DELAY_S", 2.0),
            opc=opc,
            db=db,
        )
