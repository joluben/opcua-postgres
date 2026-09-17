"""Tests del BatchWriter: orden de columnas y política drop-oldest."""

import asyncio
from types import SimpleNamespace

from connector.db.writer import BatchWriter, _COLUMNS


def test_columns_order_matches_schema():
    assert _COLUMNS == [
        "tag_id", "ts", "value_num", "value_str", "quality", "connector_id"
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
