"""Tests del buffer de spill a disco (sin BD ni OPC-UA)."""

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from connector.db.spill import SpillBuffer, enqueue_or_spill


def _cfg(tmp_path):
    return SimpleNamespace(
        spill_enabled=True,
        spill_dir=str(tmp_path),
        spill_max_mb=10,
        spill_segment_mb=1,
    )


def _record():
    now = datetime.now(timezone.utc)
    return (1, now, 42.0, None, 0, "connector-01")


def test_write_and_drain_roundtrip(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    rec = _record()

    assert sb.write(rec) is True
    assert sb.has_data() is True

    drained = sb.drain(10)
    assert len(drained) == 1
    assert drained[0][0] == rec[0]      # tag_id
    assert drained[0][1] == rec[1]      # ts preservado
    assert drained[0][2] == rec[2]      # value_num
    assert sb.has_data() is False
    sb.close()


def test_partial_drain_keeps_remainder(tmp_path):
    sb = SpillBuffer(_cfg(tmp_path), "connector-01")
    for _ in range(5):
        sb.write(_record())

    first = sb.drain(2)
    assert len(first) == 2
    assert sb.has_data() is True

    rest = sb.drain(10)
    assert len(rest) == 3
    assert sb.has_data() is False
    sb.close()


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
