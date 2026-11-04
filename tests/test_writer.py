"""Tests del BatchWriter: orden de columnas y política drop-oldest."""

import asyncio
from types import SimpleNamespace

import pytest

from connector.db.spill import ReplayBatch, ReplayToken
from connector.db.writer import BatchWriter, _COLUMNS, _rows_affected


def test_columns_order_matches_schema():
    assert _COLUMNS == [
        "event_id", "tag_id", "ts", "value_num", "value_str", "quality",
        "connector_id",
    ]


def test_requeue_drops_oldest_when_full():
    queue: asyncio.Queue = asyncio.Queue(maxsize=2)
    queue.put_nowait(("old1",))
    queue.put_nowait(("old2",))

    cfg = SimpleNamespace(data_table="t", batch_size=10, flush_interval_ms=500)
    writer = BatchWriter(pool=None, cfg=cfg, queue=queue)

    writer._requeue([("new1",)])

    assert queue.qsize() == 2
    items = [queue.get_nowait(), queue.get_nowait()]
    assert ("new1",) in items
    assert ("old1",) not in items  # el más antiguo se descartó


class _FailingPool:
    def acquire(self):
        raise RuntimeError("db down")


class _FakeSpill:
    def __init__(self) -> None:
        self.items = []

    def write(self, item) -> bool:
        self.items.append(item)
        return True


@pytest.mark.asyncio
async def test_final_drain_spills_remaining_queue_when_db_down():
    """Al apagar, la cola en memoria no se pierde: se persiste en el spill."""
    queue: asyncio.Queue = asyncio.Queue()
    for i in range(5):
        queue.put_nowait((i,))

    spill = _FakeSpill()
    cfg = SimpleNamespace(data_table="t", batch_size=2, flush_interval_ms=500)
    writer = BatchWriter(
        pool=_FailingPool(), cfg=cfg, queue=queue, spill=spill, worker_id=0,
    )
    writer.stop()

    await writer.run()

    assert queue.empty()
    assert [item[0] for item in spill.items] == [0, 1, 2, 3, 4]


def test_rows_affected_parses_insert_status():
    assert _rows_affected("INSERT 0 7") == 7
    assert _rows_affected("INSERT 0 0") == 0
    assert _rows_affected("basura") == 0


class _Tx:
    async def __aenter__(self):
        return None

    async def __aexit__(self, *_exc):
        return False


class _DedupConn:
    def __init__(self) -> None:
        self.executed = []
        self.copied = None
        self.transactions = 0

    def transaction(self):
        self.transactions += 1
        return _Tx()

    async def execute(self, query, *args):
        self.executed.append(query)
        return "INSERT 0 2"

    async def fetch(self, query, *args):
        self.executed.append(query)
        return [1, 1]

    async def copy_records_to_table(self, table, *, records, columns):
        self.copied = (table, list(records), list(columns))


@pytest.mark.asyncio
async def test_insert_dedup_uses_stage_and_on_conflict():
    cfg = SimpleNamespace(
        data_table="opc_raw_values", batch_size=10, flush_interval_ms=500,
        dedup_enabled=True,
    )
    writer = BatchWriter(pool=None, cfg=cfg, queue=asyncio.Queue())
    conn = _DedupConn()
    batch = [(i,) for i in range(3)]

    inserted = await writer._insert_dedup(conn, batch)

    assert inserted == 2
    assert conn.transactions == 1
    assert conn.copied[0] == "_opc_ingest_stage"
    assert conn.copied[2] == _COLUMNS
    create_sql = [q for q in conn.executed if "CREATE TEMP TABLE" in q]
    assert create_sql and "ON COMMIT DROP" in create_sql[0]
    assert not any("TRUNCATE" in q for q in conn.executed)
    insert_sql = [q for q in conn.executed if q.startswith("INSERT INTO")]
    assert insert_sql and "ON CONFLICT (event_id, ts)" in insert_sql[0]
    assert "DO NOTHING" in insert_sql[0]


@pytest.mark.asyncio
async def test_shutdown_drop_is_counted_when_spill_rejects():
    class _RejectingSpill:
        def write(self, _item) -> bool:
            return False

    from connector.utils import metrics

    before = metrics.SHUTDOWN_DROPPED._value.get()
    cfg = SimpleNamespace(data_table="t", batch_size=10, flush_interval_ms=500)
    writer = BatchWriter(
        pool=None, cfg=cfg, queue=asyncio.Queue(), spill=_RejectingSpill(),
    )

    writer._spill_or_drop([("a",), ("b",)])

    assert metrics.SHUTDOWN_DROPPED._value.get() - before == 2


class _ReplaySpill:
    def __init__(self, batch):
        self.batch = batch
        self.acked = []
        self.nacked = []

    async def has_data_async(self):
        return self.batch is not None

    async def drain_async(self, _max_n):
        batch, self.batch = self.batch, None
        return batch

    def ack(self, tokens):
        self.acked.append(list(tokens))

    def nack(self, tokens):
        self.nacked.append(list(tokens))


class _CopyConn:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail

    async def copy_records_to_table(self, table, *, records, columns):
        if self.fail:
            raise RuntimeError("db down")


class _Acquire:
    def __init__(self, conn) -> None:
        self.conn = conn

    async def __aenter__(self):
        return self.conn

    async def __aexit__(self, *_exc):
        return False


class _Pool:
    def __init__(self, conn) -> None:
        self.conn = conn

    def acquire(self):
        return _Acquire(self.conn)


def _writer_with(spill, conn, *, dedup=False):
    cfg = SimpleNamespace(
        data_table="opc_raw_values", batch_size=10, flush_interval_ms=500,
        dedup_enabled=dedup,
    )
    return BatchWriter(pool=_Pool(conn), cfg=cfg, queue=asyncio.Queue(), spill=spill)


@pytest.mark.asyncio
async def test_replay_acks_only_after_successful_flush():
    token = ReplayToken("spill-1.jsonl", 10)
    spill = _ReplaySpill(ReplayBatch(records=[("r",)], tokens=[token]))
    writer = _writer_with(spill, _CopyConn())

    await writer._replay_spill()

    assert spill.acked == [[token]]
    assert spill.nacked == []


@pytest.mark.asyncio
async def test_replay_nacks_on_failure_without_requeue():
    token = ReplayToken("spill-1.jsonl", 10)
    spill = _ReplaySpill(ReplayBatch(records=[("r",)], tokens=[token]))
    writer = _writer_with(spill, _CopyConn(fail=True))

    await writer._replay_spill()

    assert spill.acked == []
    assert spill.nacked == [[token]]
    assert writer._queue.empty()
