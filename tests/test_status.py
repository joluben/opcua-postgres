"""StatusCodes OPC-UA frente a la columna `quality` (INTEGER de PostgreSQL).

OPC UA define el StatusCode como uint32; `quality` es int32 con signo. Los
códigos *Bad* (≥ 0x80000000) desbordaban la columna y hacían fallar el COPY del
lote completo con `OverflowError: value out of int32 range`.
"""

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from asyncua import ua

from connector.db import spill
from connector.opc import subscription
from connector.opc.subscription import SubHandler
from connector.utils.status import to_quality

_INT32_MIN = -(2 ** 31)
_INT32_MAX = 2 ** 31 - 1


@pytest.mark.parametrize(("raw", "expected"), [
    (0, 0),                        # Good
    (0x40900000, 0x40900000),      # Uncertain: ya cabe en int32
    (0x7FFFFFFF, _INT32_MAX),      # último valor positivo representable
    (0x80000000, _INT32_MIN),      # primer código Bad
    (0x80340000, -2144075776),     # BadNodeIdUnknown
    (0xC0000000, -1073741824),     # Bad con severidad máxima
    (0xFFFFFFFF, -1),
])
def test_to_quality_fits_in_postgres_integer(raw, expected):
    assert to_quality(raw) == expected


def test_to_quality_is_idempotent():
    """Sirve también para valores ya convertidos (p. ej. leídos de un spill)."""
    for raw in (0, 0x40900000, 0x80000000, 0x80340000, 0xFFFFFFFF):
        once = to_quality(raw)
        assert to_quality(once) == once


def test_to_quality_accepts_asyncua_status_codes():
    assert to_quality(ua.StatusCodes.BadNotConnected) == -2138439680
    assert to_quality(ua.StatusCodes.Good) == 0


def _notification(status_value: int) -> SimpleNamespace:
    value = ua.DataValue(ua.Variant(1.0, ua.VariantType.Double))
    value.StatusCode = ua.StatusCode(status_value)
    value.SourceTimestamp = datetime.now(timezone.utc)
    return SimpleNamespace(monitored_item=SimpleNamespace(Value=value))


def _handler(monkeypatch) -> tuple[SubHandler, list, object]:
    captured: list = []
    monkeypatch.setattr(
        subscription, "enqueue_or_spill",
        lambda queue, record, spill: captured.append(record),
    )
    node_key = object()
    handler = SubHandler(
        queue=SimpleNamespace(),
        node_to_tag={node_key: 7},
        connector_id="connector-test",
    )
    return handler, captured, node_key


def test_subscription_stores_bad_status_within_int32(monkeypatch):
    """Regresión: un StatusCode Bad rompía el COPY de todo el lote."""
    handler, captured, node_key = _handler(monkeypatch)

    handler.datachange_notification(
        SimpleNamespace(nodeid=node_key),
        1.0,
        _notification(ua.StatusCodes.BadNodeIdUnknown),
    )

    assert len(captured) == 1
    record = captured[0]
    assert record[5] == -2144075776
    assert _INT32_MIN <= record[5] <= _INT32_MAX
    assert isinstance(record[0], uuid.UUID)


def test_subscription_keeps_good_status(monkeypatch):
    handler, captured, node_key = _handler(monkeypatch)

    handler.datachange_notification(
        SimpleNamespace(nodeid=node_key), 1.0, _notification(ua.StatusCodes.Good)
    )

    assert captured[0][5] == 0


def test_spill_decode_normalizes_unsigned_quality():
    """Un spill escrito por v1.0.2 guardaba el uint32 sin convertir."""
    line = [str(uuid.uuid4()), 7, "2026-11-20T10:00:00+00:00", 1.0, None,
            2147483648, "connector-test"]

    record = spill._decode(line)

    assert record[5] == _INT32_MIN


def test_spill_decode_legacy_line_fits_int32():
    line = [7, "2026-11-20T10:00:00+00:00", 1.0, None, 2150891520,
            "connector-test"]

    record = spill._decode(line)

    assert record[5] == -2144075776
    assert isinstance(record[0], uuid.UUID)
