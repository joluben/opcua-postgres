"""Tests del buffer de spill a disco (sin BD ni OPC-UA)."""

import asyncio
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from connector.db.spill import SpillBuffer, _decode, enqueue_or_spill


def _cfg(tmp_path):
    return SimpleNamespace(
        spill_enabled=True,
        spill_dir=str(tmp_path),
        spill_max_mb=10,
        spill_segment_mb=1,
        spill_drain_timeout_s=5,
    )


def _record(value=42.0):
    return (uuid.uuid4(), 1, datetime.now(timezone.utc), value, None, 0,
            "connector-01")


def _ack_all(sb: SpillBuffer, batch) -> None:
    sb.ack(batch.tokens)
    sb.flush()  # el worker procesa el ACK antes de continuar


def test_write_and_drain_roundtrip(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    rec = _record()

    assert sb.write(rec) is True
    assert sb.has_data() is True

    batch = sb.drain(10)
    assert len(batch.records) == 1
    got = batch.records[0]
    assert got[0] == rec[0]          # event_id preservado
    assert got[1] == rec[1]          # tag_id
    assert got[2] == rec[2]          # ts
    assert got[3] == rec[3]          # value_num
    # El drain no confirma: sin ACK los datos siguen en disco.
    assert sb.has_data() is True

    _ack_all(sb, batch)
    assert sb.has_data() is False
    sb.close()


def test_partial_drain_only_advances_on_ack(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    for _ in range(5):
        sb.write(_record())

    first = sb.drain(2)
    assert len(first.records) == 2
    assert sb.has_data() is True
    _ack_all(sb, first)

    rest = sb.drain(10)
    assert len(rest.records) == 3
    assert sb.has_data() is True
    _ack_all(sb, rest)
    assert sb.has_data() is False
    sb.close()


def test_unacked_records_survive_restart(tmp_path):
    """Simula caída tras leer pero antes de confirmar: se reentregan."""
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    rec = _record()
    sb.write(rec)
    first = sb.drain(10)
    assert len(first.records) == 1
    sb.close()  # cierre sin ACK (crash lógico)

    sb2 = SpillBuffer(_cfg(tmp_path), "connector-01")
    again = sb2.drain(10)
    assert [r[0] for r in again.records] == [rec[0]]
    _ack_all(sb2, again)
    assert sb2.has_data() is False
    sb2.close()


def test_drain_without_ack_does_not_double_deliver(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    sb.write(_record())

    first = sb.drain(10)
    assert len(first.records) == 1
    # Mientras hay un lote en vuelo no se entrega otra copia.
    assert len(sb.drain(10).records) == 0

    sb.nack(first.tokens)
    sb.flush()
    again = sb.drain(10)
    assert len(again.records) == 1
    _ack_all(sb, again)
    sb.close()


def test_legacy_six_field_line_gets_deterministic_event_id():
    now = datetime.now(timezone.utc)
    payload = [1, now.isoformat(), 42.0, None, 0, "connector-01"]
    a = _decode(payload)
    b = _decode(payload)
    assert len(a) == 7
    assert a[0] == b[0]
    assert isinstance(a[0], uuid.UUID)
    assert a[2] == now


def test_enqueue_or_spill_overflows_to_disk(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    queue: asyncio.Queue = asyncio.Queue(maxsize=1)

    enqueue_or_spill(queue, _record(), sb)   # entra en la cola
    enqueue_or_spill(queue, _record(), sb)   # cola llena -> a disco

    assert queue.qsize() == 1
    assert sb.has_data() is True
    sb.close()


def test_disabled_spill_is_noop(tmp_path):
    cfg = _cfg(tmp_path)
    cfg.spill_enabled = False
    sb = SpillBuffer(cfg, "connector-01")

    assert sb.write(_record()) is False
    assert sb.has_data() is False
