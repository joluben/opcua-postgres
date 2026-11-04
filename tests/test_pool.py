"""Tests de la construcción del contexto SSL del pool (sin conexión real a BD).

Cubren el comportamiento P0 fail-closed y la carga de CA vía ``PGSSLROOTCERT``.
"""

import datetime
import ssl

import pytest

from connector.db.pool import _build_ssl


@pytest.fixture(autouse=True)
def _clear_ca_env(monkeypatch):
    monkeypatch.delenv("PGSSLROOTCERT", raising=False)
    monkeypatch.delenv("SSLROOTCERT", raising=False)


def test_verify_full_sets_strict_context():
    ctx = _build_ssl("verify-full")
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is True
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_unknown_mode_falls_back_to_verify_full():
    """Un modo desconocido no debe degradar a CERT_NONE."""
    ctx = _build_ssl("super-secure-typo")
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is True
    assert ctx.verify_mode == ssl.CERT_REQUIRED


def test_prefer_disables_verification():
    ctx = _build_ssl("prefer")
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.check_hostname is False
    assert ctx.verify_mode == ssl.CERT_NONE


def test_disable_returns_false():
    assert _build_ssl("disable") is False


def test_missing_root_ca_fails_closed(tmp_path, monkeypatch):
    """PGSSLROOTCERT apuntando a un fichero inexistente debe fallar, no ignorarse."""
    monkeypatch.setenv("PGSSLROOTCERT", str(tmp_path / "nope.pem"))
    with pytest.raises(RuntimeError, match="PGSSLROOTCERT"):
        _build_ssl("verify-full")


def _write_self_signed_ca(path) -> None:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-ca")])
    now = datetime.datetime.now(datetime.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .sign(key, hashes.SHA256())
    )
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def test_root_ca_loaded_from_env(tmp_path, monkeypatch):
    ca_file = tmp_path / "ca.pem"
    pytest.importorskip("cryptography")
    _write_self_signed_ca(ca_file)
    monkeypatch.setenv("PGSSLROOTCERT", str(ca_file))

    ctx = _build_ssl("verify-full")
    assert isinstance(ctx, ssl.SSLContext)
    assert ctx.verify_mode == ssl.CERT_REQUIRED
