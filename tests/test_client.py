"""Tests del cliente OPC-UA: URI de aplicación anunciada en CreateSession."""

from types import SimpleNamespace

import pytest

from connector.opc import client as client_mod

_ASYNCUA_DEFAULT_URI = "urn:example.org:FreeOpcUa:opcua-asyncio"


class FakeClient:
    def __init__(self, url=None, timeout=None):
        self.url = url
        self.timeout = timeout
        self.application_uri = _ASYNCUA_DEFAULT_URI
        self.session_timeout = None
        self.connected = False

    async def connect(self):
        self.connected = True


def _cfg(**overrides):
    values = dict(
        server_url="opc.tcp://10.10.130.2:4840",
        session_timeout_ms=30000,
        application_uri=None,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.fixture
def fake_asyncua(monkeypatch):
    """Sustituye asyncua.Client y apply_security para observar el orden de uso."""
    state = SimpleNamespace(client=None, uri_at_security=None, connected_at_security=None)

    def _factory(url=None, timeout=None):
        state.client = FakeClient(url=url, timeout=timeout)
        return state.client

    async def _apply_security(client, cfg):
        state.uri_at_security = client.application_uri
        state.connected_at_security = client.connected

    monkeypatch.setattr(client_mod, "Client", _factory)
    monkeypatch.setattr(client_mod, "apply_security", _apply_security)
    return state


@pytest.mark.asyncio
async def test_default_application_uri_is_preserved(fake_asyncua):
    """Sin OPC_APPLICATION_URI el cliente conserva el default de asyncua."""
    client = await client_mod.connect(_cfg())

    assert client.application_uri == _ASYNCUA_DEFAULT_URI
    assert fake_asyncua.uri_at_security == _ASYNCUA_DEFAULT_URI
    assert client.connected is True


@pytest.mark.asyncio
async def test_application_uri_is_applied_before_security(fake_asyncua):
    client = await client_mod.connect(_cfg(application_uri="urn:acasa:conector-01"))

    assert client.application_uri == "urn:acasa:conector-01"
    assert fake_asyncua.uri_at_security == "urn:acasa:conector-01"
    # apply_security (que carga el certificado) corre antes de connect().
    assert fake_asyncua.connected_at_security is False
    assert client.connected is True
