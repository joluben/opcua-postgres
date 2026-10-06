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
# P0: trocear createMonitoredItems para no superar MaxMonitoredItems/PDU del servidor.
_SUBSCRIBE_CHUNK_SIZE = 1000


def build_datachange_filter(cfg: OpcConfig):  # noqa: ANN001, ANN202
    """Construye el DataChangeFilter a partir de OPC_DEADBAND_TYPE/VALUE.

    P0: antes ``deadband`` se parseaba pero nunca se usaba (tráfico 10-100x).
    - ``None`` o valor <= 0 → sin filtro (comportamiento anterior).
    - ``Absolute``/``Percent`` → filtro UA con DeadbandValue.
    Devuelve None si la versión de asyncua no expone la API.
    """
    dtype = (cfg.deadband_type or "None").strip()
    if dtype == "None" or cfg.deadband <= 0:
        return None
    try:
        deadband_type = getattr(ua.DeadbandType, dtype, None)
        if deadband_type is None:
            # Algunas versiones usan None_ para el valor 0.
            deadband_type = getattr(ua.DeadbandType, dtype.rstrip("_"), None)
        filt_cls = getattr(ua, "DataChangeFilter", None)
        trigger = getattr(getattr(ua, "DataChangeTrigger", object), "StatusValue", 1)
        if filt_cls is None or deadband_type is None:
            log.warning("opc_deadband_unsupported", deadband_type=dtype)
            return None
        return filt_cls(Trigger=trigger, DeadbandType=deadband_type,
                        DeadbandValue=float(cfg.deadband))
    except Exception as exc:  # noqa: BLE001
        log.warning("opc_deadband_build_failed", error=str(exc), deadband_type=dtype)
        return None


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
    """Crea la suscripción y registra los MonitoredItems de la partición local.

    P0: suscribe por bloques de 1000 nodos y aplica DataChangeFilter (deadband).
    """
    node_to_tag = {t.node.nodeid: t.tag_id for t in tags}
    handler = SubHandler(queue, node_to_tag, connector_id, spill)

    subscription = await client.create_subscription(cfg.publish_interval_ms, handler)

    nodes = [t.node for t in tags]
    datachange_filter = build_datachange_filter(cfg)
    if nodes:
        # Trocear para no saturar una sola petición CreateMonitoredItems.
        for start in range(0, len(nodes), _SUBSCRIBE_CHUNK_SIZE):
            chunk = nodes[start:start + _SUBSCRIBE_CHUNK_SIZE]
            kwargs = dict(queuesize=_QUEUE_SIZE_SERVER,
                          sampling_interval=cfg.publish_interval_ms)
            if datachange_filter is not None:
                kwargs["filter"] = datachange_filter
            await subscription.subscribe_data_change(chunk, **kwargs)
    log.info("opc_subscribed", monitored_items=len(nodes),
             publish_interval_ms=cfg.publish_interval_ms,
             deadband_type=cfg.deadband_type, deadband=cfg.deadband,
             chunks=(len(nodes) + _SUBSCRIBE_CHUNK_SIZE - 1) // _SUBSCRIBE_CHUNK_SIZE
             if nodes else 0)
    return subscription
