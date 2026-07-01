"""Motor de suscripción DataChange → cola asíncrona en memoria.

El callback ``datachange_notification`` se ejecuta dentro del event loop de asyncio
(NO en un hilo separado). Cada notificación se transforma en una tupla lista para COPY
y se encola con ``put_nowait``. Si el buffer está lleno, se aplica *drop-oldest*.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Dict, List

from asyncua import Client, ua

from ..config import OpcConfig
from ..db.spill import SpillBuffer, enqueue_or_spill
from ..utils import metrics
from ..utils.logger import get_logger
from .browser import Tag

log = get_logger(__name__)

_QUEUE_SIZE_SERVER = 100


class SubHandler:
    """Recibe notificaciones DataChange y las encola para el BatchWriter."""

    def __init__(
        self,
        queue: "asyncio.Queue",
        node_to_tag: Dict[ua.NodeId, int],
        connector_id: str,
        spill: SpillBuffer | None = None,
    ) -> None:
        self._queue = queue
        self._node_to_tag = node_to_tag
        self._connector_id = connector_id
        self._spill = spill

    def datachange_notification(self, node, val, data) -> None:  # noqa: ANN001
        metrics.VALUES_RECEIVED.inc()
        tag_id = self._node_to_tag.get(node.nodeid)
        if tag_id is None:
            return

        dv = data.monitored_item.Value
        ts = getattr(dv, "SourceTimestamp", None) or datetime.now(timezone.utc)
        status = dv.StatusCode.value if dv.StatusCode is not None else 0

        if isinstance(val, (int, float, bool)):
            value_num, value_str = float(val), None
        else:
            value_num, value_str = None, None if val is None else str(val)

        record = (
            tag_id,
            ts,
            value_num,
            value_str,
            int(status),
            self._connector_id,
        )
        enqueue_or_spill(self._queue, record, self._spill)


async def subscribe(
    client: Client,
    tags: List[Tag],
    queue: "asyncio.Queue",
    cfg: OpcConfig,
    connector_id: str,
    spill: SpillBuffer | None = None,
):
    """Crea la suscripción y registra los MonitoredItems de la partición local."""
    node_to_tag = {t.node.nodeid: t.tag_id for t in tags}
    handler = SubHandler(queue, node_to_tag, connector_id, spill)

    subscription = await client.create_subscription(cfg.publish_interval_ms, handler)

    nodes = [t.node for t in tags]
    if nodes:
        await subscription.subscribe_data_change(
            nodes,
            queuesize=_QUEUE_SIZE_SERVER,
            sampling_interval=cfg.publish_interval_ms,
        )
    log.info("opc_subscribed", monitored_items=len(nodes), publish_interval_ms=cfg.publish_interval_ms)
    return subscription
