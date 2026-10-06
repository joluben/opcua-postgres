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
                "OPC_CERTIFICATE_PATH", "OPC_PRIVATE_KEY_PATH"):
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
