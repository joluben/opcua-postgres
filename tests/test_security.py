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
