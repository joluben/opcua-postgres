"""Tests del buffer de spill a disco (sin BD ni OPC-UA)."""

import asyncio
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

from connector.db.spill import ReplayToken, SpillBuffer, _decode, enqueue_or_spill
from connector.utils import metrics


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


def test_replay_pending_gauge_tracks_inflight(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    sb.write(_record())
    sb.write(_record())

    batch = sb.drain(10)
    assert len(batch.records) == 2
    assert metrics.SPILL_REPLAY_PENDING._value.get() == 2

    _ack_all(sb, batch)
    assert metrics.SPILL_REPLAY_PENDING._value.get() == 0
    sb.close()


def _bare_spill(tmp_path, max_bytes: int) -> SpillBuffer:
    """SpillBuffer sin worker para probar el cap de forma determinista."""
    sb = object.__new__(SpillBuffer)
    sb.dir = tmp_path
    sb.max_bytes = max_bytes
    sb._cur_path = None
    sb._cur_file = None
    sb._cur_size = 0
    sb._inflight = None
    return sb


def _segment(tmp_path, name: str, nbytes: int, ack: int | None = None):
    path = tmp_path / name
    path.write_bytes(b"x" * nbytes)
    if ack is not None:
        path.with_name(path.name + ".ack").write_text(str(ack), encoding="ascii")
    return path


def test_cap_drops_acked_segment_before_unacked(tmp_path):
    sb = _bare_spill(tmp_path, max_bytes=10)
    acked = _segment(tmp_path, "spill-1.jsonl", 10, ack=10)
    pending = _segment(tmp_path, "spill-2.jsonl", 10)
    before = metrics.SPILL_DROPPED_UNACKED._value.get()

    sb._w_enforce_cap()

    assert not acked.exists()
    assert pending.exists()
    assert metrics.SPILL_DROPPED_UNACKED._value.get() == before


def test_cap_drops_unacked_only_as_last_resort_and_is_visible(tmp_path):
    sb = _bare_spill(tmp_path, max_bytes=5)
    acked = _segment(tmp_path, "spill-3.jsonl", 10, ack=10)
    pending = _segment(tmp_path, "spill-4.jsonl", 10)
    before = metrics.SPILL_DROPPED_UNACKED._value.get()

    sb._w_enforce_cap()

    assert not acked.exists()
    assert not pending.exists()
    assert metrics.SPILL_DROPPED_UNACKED._value.get() - before == 1


def test_cap_never_drops_inflight_segment(tmp_path):
    sb = _bare_spill(tmp_path, max_bytes=5)
    inflight = _segment(tmp_path, "spill-5.jsonl", 20)
    sb._inflight = [ReplayToken(inflight.name, 0)]

    sb._w_enforce_cap()

    assert inflight.exists()


def test_drain_timeout_does_not_lose_records(tmp_path):
    """Si el worker no responde a tiempo, el lote se reentrega en el siguiente ciclo."""
    import time

    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    sb.drain_timeout_s = 0.05
    sb.write(_record())
    sb.flush()

    original = sb._w_drain

    def slow_drain(max_n):
        time.sleep(0.2)
        return original(max_n)

    sb._w_drain = slow_drain
    assert sb.drain(10).records == []  # timeout
    sb.flush()                         # espera al worker y procesa el cancel
    sb._w_drain = original
    sb.drain_timeout_s = 5.0

    again = sb.drain(10)
    assert len(again.records) == 1
    _ack_all(sb, again)
    sb.close()
