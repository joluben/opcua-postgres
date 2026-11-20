"""Tests de configuración y seguridad (sin dependencias externas)."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from connector.config import Config, ConfigError, _read_secret
from connector.opc import security


def _base_env(monkeypatch):
    monkeypatch.setenv("OPC_SERVER_URL", "opc.tcp://localhost:4840")
    monkeypatch.setenv("POSTGRES_HOST", "db.internal.example")
    monkeypatch.setenv("POSTGRES_DB", "scada_db")
    monkeypatch.setenv("POSTGRES_USER", "connector_user")
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.setenv("POSTGRES_SCHEMA", "opcua")
    monkeypatch.setenv("CONNECTOR_ID", "connector-01")
    # P0: aislar de *_FILE del host para que los defaults fail-closed no hereden
    # ficheros inexistentes del entorno local.
    for var in ("OPC_PASSWORD_FILE", "POSTGRES_PASSWORD_FILE",
                "OPC_CERTIFICATE_PATH", "OPC_PRIVATE_KEY_PATH",
                "OPC_TAG_OFFSET", "OPC_TAG_LIMIT", "OPC_ALLOW_TAG_OMISSION",
                "OPC_APPLICATION_URI",
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


# ── POSTGRES_SCHEMA: obligatorio y validado ────────────────────────────────

def test_schema_is_required_and_never_defaults_to_public(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.delenv("POSTGRES_SCHEMA", raising=False)

    with pytest.raises(ConfigError, match="POSTGRES_SCHEMA"):
        Config.from_env()


def test_schema_is_read_from_environment(monkeypatch):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("POSTGRES_SCHEMA", "catalog")

    assert Config.from_env().db.schema == "catalog"


@pytest.mark.parametrize(
    "raw", ["public; DROP TABLE x", "opc-ua", "1schema", "", "a.b"]
)
def test_invalid_schema_is_rejected(monkeypatch, raw):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("POSTGRES_SCHEMA", raw)

    with pytest.raises(ConfigError, match="POSTGRES_SCHEMA"):
        Config.from_env()


# ── OPC_APPLICATION_URI ─────────────────────────────────────────────────────

def test_application_uri_defaults_to_none(monkeypatch):
    """Sin la variable, el cliente conserva la URI por defecto de asyncua."""
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")

    assert Config.from_env().opc.application_uri is None


@pytest.mark.parametrize(
    "raw",
    ["urn:acasa-opcua0:UnifiedAutomation:UaExpert", "https://host.example/conector"],
)
def test_application_uri_is_read_from_environment(monkeypatch, raw):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("OPC_APPLICATION_URI", raw)

    assert Config.from_env().opc.application_uri == raw


@pytest.mark.parametrize(
    "raw", ["opcua-sin-esquema", "urn:", "urn:con espacio", "urn:salto\nde-linea"]
)
def test_invalid_application_uri_is_rejected(monkeypatch, raw):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("OPC_APPLICATION_URI", raw)

    with pytest.raises(ConfigError, match="OPC_APPLICATION_URI"):
        Config.from_env()


@pytest.fixture
def log_events(monkeypatch):
    """Captura los eventos structlog emitidos por connector.opc.security."""
    events = []

    def _record(event, **kwargs):
        events.append((event, kwargs))

    monkeypatch.setattr(
        security, "log",
        SimpleNamespace(info=_record, warning=_record, error=_record),
    )
    return events


def _write_cert(tmp_path, uris, *, der=True):
    """Certificado autofirmado de prueba; ``uris=None`` → sin extensión SAN."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "connector-test")])
    now = datetime.now(timezone.utc)
    builder = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
    )
    if uris is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.UniformResourceIdentifier(uri) for uri in uris]
                + [x509.DNSName("connector-host")]
            ),
            critical=False,
        )
    cert = builder.sign(key, hashes.SHA256())
    encoding = serialization.Encoding.DER if der else serialization.Encoding.PEM
    path = tmp_path / ("client_cert.der" if der else "client_cert.pem")
    path.write_bytes(cert.public_bytes(encoding))
    return path


def _opc_cfg(monkeypatch, cert_path, application_uri):
    _base_env(monkeypatch)
    monkeypatch.setenv("OPC_SECURITY_MODE", "None")
    monkeypatch.setenv("OPC_CERTIFICATE_PATH", str(cert_path))
    if application_uri is not None:
        monkeypatch.setenv("OPC_APPLICATION_URI", application_uri)
    return Config.from_env().opc


@pytest.mark.parametrize(("der", "uri"), [
    (True, "urn:acasa:conector-01"),
    (False, "urn:acasa:conector-02"),
])
def test_matching_application_uri_does_not_warn(monkeypatch, tmp_path, log_events,
                                                der, uri):
    cert = _write_cert(tmp_path, [uri], der=der)

    security._check_application_uri(_opc_cfg(monkeypatch, cert, uri))

    assert log_events == []


@pytest.mark.parametrize("der", [True, False])
def test_mismatched_application_uri_warns(monkeypatch, tmp_path, log_events, der):
    cert = _write_cert(tmp_path, ["urn:otra:aplicacion"], der=der)

    security._check_application_uri(
        _opc_cfg(monkeypatch, cert, "urn:acasa:conector-01")
    )

    assert [event for event, _ in log_events] == ["opc_application_uri_mismatch"]
    _, kwargs = log_events[0]
    assert kwargs["application_uri"] == "urn:acasa:conector-01"
    assert kwargs["certificate_uris"] == ["urn:otra:aplicacion"]


def test_certificate_without_san_warns(monkeypatch, tmp_path, log_events):
    cert = _write_cert(tmp_path, None)

    security._check_application_uri(
        _opc_cfg(monkeypatch, cert, "urn:acasa:conector-01")
    )

    assert [event for event, _ in log_events] == ["opc_application_uri_mismatch"]
    assert log_events[0][1]["certificate_uris"] == []


def test_unreadable_certificate_only_warns(monkeypatch, tmp_path, log_events):
    """Un certificado ilegible no debe abortar el arranque (diagnóstico best-effort)."""
    cert = tmp_path / "broken_cert.der"
    cert.write_bytes(b"no soy un certificado")

    security._check_application_uri(
        _opc_cfg(monkeypatch, cert, "urn:acasa:conector-01")
    )

    assert [event for event, _ in log_events] == ["opc_certificate_read_error"]


def test_no_application_uri_skips_certificate_check(monkeypatch, tmp_path, log_events):
    cert = tmp_path / "broken_cert.der"
    cert.write_bytes(b"no soy un certificado")

    security._check_application_uri(_opc_cfg(monkeypatch, cert, None))

    assert log_events == []
