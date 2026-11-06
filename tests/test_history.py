"""Tests de la recuperación de huecos OPC (Fase 6)."""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from asyncua import ua

from connector.db.spill import _legacy_event_id
from connector.opc.history import (
    RESULT_ERROR,
    RESULT_GAP_TOO_LARGE,
    RESULT_NO_DATA,
    RESULT_OK,
    RESULT_UNSUPPORTED,
    datavalue_to_record,
    load_watermarks,
    recover_gaps,
)
from connector.utils import metrics
from connector.utils.event_id import deterministic_event_id


def _dv(value, ts, status=0):
    return ua.DataValue(
        Value=ua.Variant(value),
        StatusCode=ua.StatusCode(status),
        SourceTimestamp=ts,
    )


def _result(values=None, status=0, continuation=None):
    return ua.HistoryReadResult(
        StatusCode=ua.StatusCode(status),
        ContinuationPoint=continuation,
        HistoryData=ua.HistoryData(DataValues=list(values or [])),
    )


class _FakeNode:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    async def history_read(self, details, continuation=None):
        self.calls.append((details.StartTime, details.EndTime, continuation))
        return self.pages.pop(0)


def _tag(tag_id, node):
    return SimpleNamespace(tag_id=tag_id, node=node, node_id=f"ns=2;s={tag_id}")


async def _recover(tag, watermarks, cutoff, *, max_gap_s=3600, batch=100):
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    results = await recover_gaps(
        tags=[tag],
        watermarks=watermarks,
        cutoff=cutoff,
        connector_id="connector-test",
        queue=queue,
        spill=None,
        max_gap_s=max_gap_s,
        batch_size=batch,
        timeout_s=2.0,
    )
    return results, queue


def _drain(queue):
    out = []
    while True:
        try:
            out.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            return out


def test_deterministic_event_id_matches_legacy_format():
    ts = datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    a = deterministic_event_id(7, ts, 1.5, None, 0, "c1")
    b = _legacy_event_id(7, ts, 1.5, None, 0, "c1")
    assert a == b
    assert a == deterministic_event_id(7, ts, 1.5, None, 0, "c1")


@pytest.mark.asyncio
async def test_load_watermarks_groups_by_tag():
    class _Conn:
        async def fetch(self, _query):
            return [
                {"tag_id": 1, "last_ts": datetime(2026, 1, 1, tzinfo=timezone.utc)},
                {"tag_id": 2, "last_ts": None},
            ]

    class _Acquire:
        async def __aenter__(self):
            return _Conn()

        async def __aexit__(self, *_exc):
            return False

    class _Pool:
        def acquire(self):
            return _Acquire()

    marks = await load_watermarks(_Pool(), "opc_raw_values")
    assert marks[1] == datetime(2026, 1, 1, tzinfo=timezone.utc)
    assert 2 not in marks


@pytest.mark.asyncio
async def test_recover_gap_reads_and_ingests_deterministic_records():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(seconds=30)
    node = _FakeNode([_result([
        _dv(1.0, last + timedelta(seconds=10)),
        _dv(2.0, last + timedelta(seconds=20)),
    ])])

    results, queue = await _recover(_tag(1, node), {1: last}, now)

    assert results[0].result == RESULT_OK
    assert results[0].values == 2
    records = _drain(queue)
    assert [r[3] for r in records] == [1.0, 2.0]
    for record in records:
        assert record[0] == deterministic_event_id(
            record[1], record[2], record[3], record[4], record[5], record[6]
        )
    # Rango inclusivo: no se pierde un valor exactamente en el watermark.
    assert node.calls[0][0] == last
    assert node.calls[0][1] == now


@pytest.mark.asyncio
async def test_recover_gap_follows_continuation_points():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(minutes=5)
    node = _FakeNode([
        _result([
            _dv(1.0, last + timedelta(seconds=1)),
            _dv(2.0, last + timedelta(seconds=2)),
        ], continuation=b"cp-1"),
        _result([_dv(3.0, last + timedelta(seconds=3))]),
    ])

    results, queue = await _recover(_tag(1, node), {1: last}, now, batch=2)

    assert results[0].result == RESULT_OK
    assert results[0].values == 3
    assert len(node.calls) == 2
    assert node.calls[0][2] is None
    assert node.calls[1][2] == b"cp-1"
    assert [r[3] for r in _drain(queue)] == [1.0, 2.0, 3.0]


@pytest.mark.asyncio
async def test_recover_gap_without_history_is_counted_as_unrecovered():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(seconds=10)
    node = _FakeNode([_result([], status=ua.StatusCodes.BadNoData)])
    before = metrics.HISTORY_GAP_UNRECOVERED._value.get()

    results, _ = await _recover(_tag(1, node), {1: last}, now)

    assert results[0].result == RESULT_NO_DATA
    assert metrics.HISTORY_GAP_UNRECOVERED._value.get() - before == 1


@pytest.mark.asyncio
async def test_recover_gap_unsupported_server_is_counted():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(seconds=10)
    node = _FakeNode([_result([], status=ua.StatusCodes.BadHistoryOperationUnsupported)])
    before = metrics.HISTORY_GAP_UNRECOVERED._value.get()

    results, _ = await _recover(_tag(1, node), {1: last}, now)

    assert results[0].result == RESULT_UNSUPPORTED
    assert metrics.HISTORY_GAP_UNRECOVERED._value.get() - before == 1


@pytest.mark.asyncio
async def test_gap_larger_than_limit_is_not_read_but_is_counted():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(days=30)
    node = _FakeNode([_result([_dv(1.0, now)])])

    results, queue = await _recover(
        _tag(1, node), {1: last}, now, max_gap_s=60
    )

    assert results[0].result == RESULT_GAP_TOO_LARGE
    assert node.calls == []
    assert _drain(queue) == []


@pytest.mark.asyncio
async def test_history_read_error_is_isolated():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(seconds=10)

    class _BoomNode:
        async def history_read(self, *_a, **_k):
            raise RuntimeError("history unavailable")

    results, _ = await _recover(_tag(1, _BoomNode()), {1: last}, now)

    assert results[0].result == RESULT_ERROR
    assert "history unavailable" in (results[0].error or "")


@pytest.mark.asyncio
async def test_recovery_is_idempotent_by_event_id():
    now = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    last = now - timedelta(seconds=10)

    def _pages():
        return [_result([_dv(5.0, last + timedelta(seconds=5))])]

    _, queue1 = await _recover(_tag(1, _FakeNode(_pages())), {1: last}, now)
    _, queue2 = await _recover(_tag(1, _FakeNode(_pages())), {1: last}, now)

    assert _drain(queue1)[0][0] == _drain(queue2)[0][0]


def test_datavalue_without_timestamp_is_skipped():
    assert datavalue_to_record(
        _dv(1.0, None), tag_id=1, connector_id="c"
    ) is None
