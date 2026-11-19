"""Tests de configuración y seguridad (sin dependencias externas)."""

import pytest

from connector.config import Config, ConfigError, _read_secret


def _base_env(monkeypatch):
    monkeypatch.setenv("OPC_SERVER_URL", "opc.tcp://localhost:4840")
    monkeypatch.setenv("POSTGRES_HOST", "db.internal.example")
    monkeypatch.setenv("POSTGRES_DB", "scada_db")
    monkeypatch.setenv("POSTGRES_USER", "connector_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("CONNECTOR_ID", "connector-01")
    # P0: aislar de *_FILE del host para que los defaults fail-closed no hereden
    # ficheros inexistentes del entorno local.
    for var in ("OPC_PASSWORD_FILE", "POSTGRES_PASSWORD_FILE",
                "OPC_CERTIFICATE_PATH", "OPC_PRIVATE_KEY_PATH",
                "OPC_TAG_OFFSET", "OPC_TAG_LIMIT", "OPC_ALLOW_TAG_OMISSION",
                "POSTGRES_TIMESCALE_CHUNK_INTERVAL",
                "POSTGRES_TIMESCALE_COMPRESSION_INTERVAL",
                "POSTGRES_TIMESCALE_RETENTION_INTERVAL"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.delenv("OPC_SECURITY_POLICY", raising=False)
    monkeypatch.delenv("OPC_DEADBAND_TYPE", raising=False)
    monkeypatch.delenv("POSTGRES_SSL_MODE", raising=False)


def test_sign_and_encrypt_requires_certificates(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "SignAndEncrypt")
    monkeypatch.delenv("OPC_CERTIFICATE_PATH", raising=False)
    monkeypatch.delenv("OPC_PRIVATE_KEY_PATH", raising=False)

    with pytest.raises(ConfigError, match="OPC_CERTIFICATE_PATH"):
        Config.from_env()


def test_security_none_does_not_require_certs(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")

    cfg = Config.from_env()
    assert cfg.opc.security_mode == "None"
    assert cfg.db.host == "db.internal.example"


def test_metrics_enabled_defaults_to_true(monkeypatch):
    """Sin METRICS_ENABLED el endpoint de observabilidad queda activo."""
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.delenv("METRICS_ENABLED", raising=False)

    assert Config.from_env().metrics_enabled is True


@pytest.mark.parametrize("raw", ["false", "0", "no", "off", "FALSE"])
def test_metrics_can_be_disabled(monkeypatch, raw):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("METRICS_ENABLED", raw)

    assert Config.from_env().metrics_enabled is False


def test_metrics_enabled_explicit_true(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("METRICS_ENABLED", "true")

    assert Config.from_env().metrics_enabled is True


def test_read_secret_file_takes_precedence(monkeypatch, tmp_path):
    secret_file = tmp_path / "pw.txt"
    secret_file.write_text("from-file\n")
    monkeypatch.setenv("POSTGRES_PASSWORD", "from-env")
    monkeypatch.setenv("POSTGRES_PASSWORD_FILE", str(secret_file))

    assert _read_secret("POSTGRES_PASSWORD") == "from-file"


def test_default_is_fail_closed_signandencrypt(monkeypatch):
    """P0: sin OPC_SECURITY_MODE explícito debe exigir certificados."""
    _base_env(monkeypatch)
    monkeypatch.delenv("OPC_SECURITY_MODE", raising=False)
    with pytest.raises(ConfigError, match="OPC_CERTIFICATE_PATH"):
        Config.from_env()


def test_deprecated_policy_rejected(monkeypatch):
    """P0: Basic128Rsa15/Basic256 obsoletas deben rechazarse."""
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "SignAndEncrypt")
    monkeypatch.setenv("OPC_SECURITY_POLICY", "Basic128Rsa15")
    monkeypatch.setenv("OPC_CERTIFICATE_PATH", "/tmp/cert.pem")
    monkeypatch.setenv("OPC_PRIVATE_KEY_PATH", "/tmp/key.pem")
    with pytest.raises(ConfigError, match="OPC_SECURITY_POLICY"):
        Config.from_env()


def test_unknown_ssl_mode_rejected(monkeypatch):
    """P0: POSTGRES_SSL_MODE desconocido debe fallar en lugar de degradar."""
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("POSTGRES_SSL_MODE", "super-secure-typo")
    with pytest.raises(ConfigError, match="POSTGRES_SSL_MODE"):
        Config.from_env()


def test_tag_omission_is_not_allowed_by_default(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.delenv("OPC_TAG_OFFSET", raising=False)
    monkeypatch.delenv("OPC_TAG_LIMIT", raising=False)
    monkeypatch.delenv("OPC_ALLOW_TAG_OMISSION", raising=False)

    cfg = Config.from_env()

    assert cfg.opc.tag_offset == 0
    assert cfg.opc.tag_limit == 5000
    assert cfg.opc.allow_tag_omission is False


def test_tag_omission_requires_explicit_opt_in(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    # Definir offset/limit NO debe habilitar el recorte silencioso.
    monkeypatch.setenv("OPC_TAG_OFFSET", "0")
    monkeypatch.setenv("OPC_TAG_LIMIT", "1000")
    monkeypatch.delenv("OPC_ALLOW_TAG_OMISSION", raising=False)

    cfg = Config.from_env()

    assert cfg.opc.tag_limit == 1000
    assert cfg.opc.allow_tag_omission is False


def test_tag_omission_enabled_explicitly(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("OPC_ALLOW_TAG_OMISSION", "true")

    cfg = Config.from_env()

    assert cfg.opc.allow_tag_omission is True


def test_timescale_policy_intervals_have_safe_defaults(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")

    db = Config.from_env().db

    assert db.timescale_chunk_interval == "1 day"
    assert db.timescale_compression_interval == "1 month"
    assert db.timescale_retention_interval == "365 days"


def test_timescale_policy_intervals_are_configurable(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("POSTGRES_TIMESCALE_CHUNK_INTERVAL", "7 days")
    monkeypatch.setenv("POSTGRES_TIMESCALE_COMPRESSION_INTERVAL", "14 days")
    monkeypatch.setenv("POSTGRES_TIMESCALE_RETENTION_INTERVAL", "730 days")

    db = Config.from_env().db

    assert db.timescale_chunk_interval == "7 days"
    assert db.timescale_compression_interval == "14 days"
    assert db.timescale_retention_interval == "730 days"


@pytest.mark.parametrize("raw", ["disabled", "off", "none", "0", ""])
def test_timescale_policies_can_be_delegated_to_dba(monkeypatch, raw):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("POSTGRES_TIMESCALE_CHUNK_INTERVAL", raw)
    monkeypatch.setenv("POSTGRES_TIMESCALE_COMPRESSION_INTERVAL", raw)
    monkeypatch.setenv("POSTGRES_TIMESCALE_RETENTION_INTERVAL", raw)

    db = Config.from_env().db

    assert db.timescale_chunk_interval is None
    assert db.timescale_compression_interval is None
    assert db.timescale_retention_interval is None


def test_invalid_timescale_interval_is_rejected(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv(
        "POSTGRES_TIMESCALE_RETENTION_INTERVAL", "1 day'; DROP TABLE x;--"
    )

    with pytest.raises(ConfigError, match="POSTGRES_TIMESCALE_RETENTION_INTERVAL"):
        Config.from_env()
