"""Carga y validación de la configuración desde variables de entorno.

Reglas:
- Ninguna credencial se hardcodea.
- Soporta la convención Docker Secrets ``<VAR>_FILE`` (p.ej. ``POSTGRES_PASSWORD_FILE``):
  si existe el fichero, su contenido tiene prioridad sobre la variable en claro.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


class ConfigError(RuntimeError):
    """Error de configuración (variable obligatoria ausente o inválida)."""


def _read_secret(name: str, default: Optional[str] = None) -> Optional[str]:
    """Lee ``<name>`` priorizando el fichero indicado por ``<name>_FILE``."""
    file_path = os.getenv(f"{name}_FILE")
    if file_path and Path(file_path).is_file():
        return Path(file_path).read_text(encoding="utf-8").strip()
    return os.getenv(name, default)


def _required(name: str) -> str:
    value = _read_secret(name)
    if value is None or value == "":
        raise ConfigError(f"Variable de entorno obligatoria ausente: {name}")
    return value


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:  # noqa: BLE001
        raise ConfigError(f"{name} debe ser entero, recibido: {raw!r}") from exc


def _float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:  # noqa: BLE001
        raise ConfigError(f"{name} debe ser numérico, recibido: {raw!r}") from exc


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
    use_timescale: bool
    spill_enabled: bool
    spill_dir: str
    spill_max_mb: int
    spill_segment_mb: int


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
        security_mode = os.getenv("OPC_SECURITY_MODE", "None")
        cert_path = _opt("OPC_CERTIFICATE_PATH")
        key_path = _opt("OPC_PRIVATE_KEY_PATH")

        if security_mode != "None" and (not cert_path or not key_path):
            raise ConfigError(
                "OPC_CERTIFICATE_PATH y OPC_PRIVATE_KEY_PATH son obligatorios "
                f"cuando OPC_SECURITY_MODE={security_mode!r}"
            )

        ns_raw = os.getenv("OPC_NAMESPACE_INDEX")
        opc = OpcConfig(
            server_url=_required("OPC_SERVER_URL"),
            username=_opt("OPC_USERNAME"),
            password=_read_secret("OPC_PASSWORD"),
            security_policy=os.getenv("OPC_SECURITY_POLICY", "Basic256Sha256"),
            security_mode=security_mode,
            certificate_path=cert_path,
            private_key_path=key_path,
            publish_interval_ms=_int("OPC_PUBLISH_INTERVAL_MS", 500),
            deadband=_float("OPC_DATACHANGE_DEADBAND", 0.0),
            deadband_type=os.getenv("OPC_DEADBAND_TYPE", "None"),
            namespace_index=int(ns_raw) if ns_raw else None,
            node_id_filter=_opt("OPC_NODE_ID_FILTER"),
            tag_offset=_int("OPC_TAG_OFFSET", 0),
            tag_limit=_int("OPC_TAG_LIMIT", 5000),
            session_timeout_ms=_int("OPC_SESSION_TIMEOUT_MS", 30000),
            queue_max_size=_int("OPC_QUEUE_MAX_SIZE", 500000),
        )

        db = DbConfig(
            host=_required("POSTGRES_HOST"),
            port=_int("POSTGRES_PORT", 5432),
            database=_required("POSTGRES_DB"),
            user=_required("POSTGRES_USER"),
            password=_required("POSTGRES_PASSWORD"),
            ssl_mode=os.getenv("POSTGRES_SSL_MODE", "prefer"),
            catalog_table=os.getenv("POSTGRES_CATALOG_TABLE", "opc_tags_catalog"),
            data_table=os.getenv("POSTGRES_DATA_TABLE", "opc_raw_values"),
            batch_size=_int("POSTGRES_BATCH_SIZE", 1000),
            flush_interval_ms=_int("POSTGRES_FLUSH_INTERVAL_MS", 500),
            pool_min=_int("POSTGRES_POOL_MIN", 2),
            pool_max=_int("POSTGRES_POOL_MAX", 10),
            statement_cache_size=_int("POSTGRES_STATEMENT_CACHE_SIZE", 100),
            use_timescale=_bool("POSTGRES_USE_TIMESCALE", True),
            spill_enabled=_bool("POSTGRES_SPILL_ENABLED", True),
            spill_dir=os.getenv("POSTGRES_SPILL_DIR", "/var/lib/connector/spill"),
            spill_max_mb=_int("POSTGRES_SPILL_MAX_MB", 1024),
            spill_segment_mb=_int("POSTGRES_SPILL_SEGMENT_MB", 64),
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
