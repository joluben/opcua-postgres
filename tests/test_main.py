"""Tests del ciclo de vida de la sesión y del apagado ordenado (Fase 1)."""

import asyncio
from types import SimpleNamespace

import pytest

from connector import main as main_mod


class FakePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


def _cfg(**overrides):
    db = SimpleNamespace(
        num_writers=2,
        data_table="opc_raw_values",
        batch_size=10,
        flush_interval_ms=500,
    )
    values = dict(
        db=db,
        opc=SimpleNamespace(),
        connector_id="connector-test",
        reconnect_max_retries=0,
        reconnect_base_delay_s=0.0,
        shutdown_timeout_s=1.0,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def _pending_writer_tasks():
    return [
        t for t in asyncio.all_tasks()
        if not t.done() and t.get_name().startswith("batch-writer-")
    ]


@pytest.mark.asyncio
async def test_writers_stopped_when_opc_connect_fails(monkeypatch):
    pool = FakePool()

    async def fake_create_pool(_cfg):
        return pool

    async def fake_initialize_schema(_pool, _cfg):
        return None

    async def fake_connect(_cfg):
        raise RuntimeError("opc down")

    monkeypatch.setattr(main_mod.db_pool, "create_pool", fake_create_pool)
    monkeypatch.setattr(main_mod, "initialize_schema", fake_initialize_schema)
    monkeypatch.setattr(main_mod.opc_client, "connect", fake_connect)

    with pytest.raises(RuntimeError, match="opc down"):
        await main_mod._run_opc_session(
            _cfg(), asyncio.Queue(), main_mod.metrics.HealthState(), None,
            asyncio.Event(),
        )

    assert pool.closed is True
    assert _pending_writer_tasks() == []


@pytest.mark.asyncio
async def test_writers_stopped_when_discovery_fails(monkeypatch):
    pool = FakePool()
    client = SimpleNamespace(disconnected=False)

    async def fake_create_pool(_cfg):
        return pool

    async def fake_initialize_schema(_pool, _cfg):
        return None

    async def fake_connect(_cfg):
        return client

    async def fake_discover(*_args, **_kwargs):
        raise RuntimeError("browse failed")

    async def fake_disconnect(self=None):
        client.disconnected = True

    monkeypatch.setattr(main_mod.db_pool, "create_pool", fake_create_pool)
    monkeypatch.setattr(main_mod, "initialize_schema", fake_initialize_schema)
    monkeypatch.setattr(main_mod.opc_client, "connect", fake_connect)
    monkeypatch.setattr(main_mod, "discover_and_partition", fake_discover)

    # El cliente falso necesita disconnect().
    client.disconnect = fake_disconnect

    with pytest.raises(RuntimeError, match="browse failed"):
        await main_mod._run_opc_session(
            _cfg(), asyncio.Queue(), main_mod.metrics.HealthState(), None,
            asyncio.Event(),
        )

    assert client.disconnected is True
    assert pool.closed is True
    assert _pending_writer_tasks() == []


@pytest.mark.asyncio
async def test_wait_until_disconnected_honours_stop_event():
    class FakeClient:
        def __init__(self) -> None:
            self.checks = 0

        async def check_connection(self) -> None:
            self.checks += 1

    client = FakeClient()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        main_mod._wait_until_disconnected(client, stop_event)
    )
    await asyncio.sleep(0.01)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1.0)
    # No ha hecho falta esperar al sondeo de 5 s.
    assert client.checks == 0


@pytest.mark.asyncio
async def test_stop_writers_cancels_hanging_tasks():
    class FakeWriter:
        def __init__(self) -> None:
            self.stopped = False

        def stop(self) -> None:
            self.stopped = True

    async def hang() -> None:
        await asyncio.sleep(3600)

    writer = FakeWriter()
    task = asyncio.create_task(hang(), name="batch-writer-hang")
    await asyncio.sleep(0)

    await asyncio.wait_for(
        main_mod._stop_writers([writer], [task], timeout_s=0.05),
        timeout=1.0,
    )

    assert writer.stopped is True
    assert task.cancelled() is True
