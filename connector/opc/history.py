"""Recuperación de huecos OPC-UA perdidos durante una reconexión.

Cada sesión OPC crea una suscripción nueva; los cambios ocurridos mientras no
había sesión no llegan por ``DataChange``. Este módulo cierra ese hueco con
``HistoryRead``:

1. Obtiene el último ``ts`` persistido por tag desde PostgreSQL.
2. Lee el histórico raw del nodo entre ese ``ts`` y un corte fijado antes de
   crear la suscripción.
3. Inyecta los valores por el mismo camino que el tiempo real
   (``enqueue_or_spill`` → ``BatchWriter`` → ``ON CONFLICT``).

El ``event_id`` es determinista (contenido), así que reintentos y solapes con
datos en tiempo real se deduplican en PostgreSQL.

Limitaciones deliberadas:

- El servidor debe soportar ``HistoryRead`` y el nodo debe estar historizado.
  Si no, el hueco se registra como no recuperado (nunca se ignora).
- La lectura está acotada por ``max_gap_s``, ``batch_size`` y ``timeout_s``.
- Se ejecuta como tarea de baja prioridad: cede el event loop entre páginas y
  usa el spill si la cola se llena.
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Mapping, Optional, Sequence

import asyncpg
from asyncua import ua

from ..db.spill import SpillBuffer, enqueue_or_spill
from ..utils import metrics
from ..utils.event_id import deterministic_event_id
from ..utils.logger import get_logger

log = get_logger(__name__)

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_MAX_PAGES = 10_000
_CAPACITY_SLEEP_S = 0.05

RESULT_OK = "ok"
RESULT_NO_DATA = "no_data"
RESULT_UNSUPPORTED = "unsupported"
RESULT_ERROR = "error"
RESULT_GAP_TOO_LARGE = "gap_too_large"

_UNSUPPORTED_CODES = {
    ua.StatusCodes.BadHistoryOperationUnsupported,
    ua.StatusCodes.BadHistoryOperationInvalid,
    ua.StatusCodes.BadNotReadable,
    ua.StatusCodes.BadUserAccessDenied,
    ua.StatusCodes.BadNodeIdUnknown,
    ua.StatusCodes.BadDataEncodingUnsupported,
}
_NO_DATA_CODES = {
    ua.StatusCodes.BadNoData,
    ua.StatusCodes.BadNoDataAvailable,
}


@dataclass(slots=True)
class GapResult:
    """Resultado de intentar recuperar el hueco de un tag."""

    tag_id: int
    result: str
    last_ts: Optional[datetime]
    cutoff: datetime
    gap_s: float
    values: int = 0
    error: Optional[str] = None


def _validate_table(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"Nombre de tabla inválido: {name!r}")
    return name


async def load_watermarks(
    pool: asyncpg.Pool, data_table: str
) -> Dict[int, datetime]:
    """Último ``ts`` persistido por tag (fuente: PostgreSQL).

    Se ejecuta una vez por sesión. Si la tabla está vacía devuelve ``{}``.
    """
    table = _validate_table(data_table)
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT tag_id, MAX(ts) AS last_ts FROM {table} GROUP BY tag_id"
        )
    result: Dict[int, datetime] = {}
    for row in rows:
        last_ts = row["last_ts"]
        if last_ts is None:
            continue
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        result[int(row["tag_id"])] = last_ts
    return result


def _status_value(result) -> int:  # noqa: ANN001
    status = getattr(result, "StatusCode", None)
    if status is None:
        return 0
    return int(getattr(status, "value", status))


def _status_allowed(code: int, allowed: set[int]) -> bool:
    return any(int(value) == code for value in allowed)


def _variant_value(data_value) -> object:  # noqa: ANN001
    value = getattr(data_value, "Value", None)
    if isinstance(value, ua.Variant):
        return value.Value
    return value


def datavalue_to_record(
    data_value,  # noqa: ANN001
    *,
    tag_id: int,
    connector_id: str,
) -> Optional[tuple]:
    """Convierte un ``ua.DataValue`` histórico en un registro del pipeline.

    Devuelve ``None`` si no hay ``SourceTimestamp`` (no se puede ubicar en la
    serie temporal). El valor se normaliza igual que en tiempo real para que el
    ``event_id`` determinista coincida en un solape.
    """
    ts = getattr(data_value, "SourceTimestamp", None)
    if ts is None:
        ts = getattr(data_value, "ServerTimestamp", None)
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)

    value = _variant_value(data_value)
    if isinstance(value, (int, float, bool)):
        value_num, value_str = float(value), None
    else:
        value_num, value_str = None, None if value is None else str(value)

    status = getattr(data_value, "StatusCode", None)
    quality = int(getattr(status, "value", status) or 0)
    event_id = deterministic_event_id(
        tag_id, ts, value_num, value_str, quality, connector_id
    )
    return (
        event_id, tag_id, ts, value_num, value_str, quality, connector_id,
    )


def _build_details(
    start: datetime, end: datetime, batch_size: int
) -> ua.ReadRawModifiedDetails:
    details = ua.ReadRawModifiedDetails()
    details.IsReadModified = False
    details.StartTime = start
    details.EndTime = end
    details.NumValuesPerNode = batch_size
    details.ReturnBounds = False
    return details


async def _breathe(queue) -> None:  # noqa: ANN001
    """Cede el loop; si la cola está muy llena, espera un poco (baja prioridad)."""
    try:
        maxsize = getattr(queue, "maxsize", 0) or 0
        pressure = maxsize > 0 and queue.qsize() / maxsize >= 0.8
    except Exception:  # noqa: BLE001
        pressure = False
    await asyncio.sleep(_CAPACITY_SLEEP_S if pressure else 0)


async def _read_and_ingest(
    node,  # noqa: ANN001
    *,
    tag_id: int,
    connector_id: str,
    start: datetime,
    end: datetime,
    batch_size: int,
    timeout_s: float,
    queue,  # noqa: ANN001
    spill: Optional[SpillBuffer],
) -> tuple[str, int, Optional[str]]:
    """Lee el histórico de un nodo y lo inyecta. Devuelve (resultado, valores, error)."""
    details = _build_details(start, end, batch_size)
    continuation: Optional[bytes] = None
    values = 0
    pages = 0

    while True:
        try:
            result = await asyncio.wait_for(
                node.history_read(details, continuation), timeout=timeout_s
            )
        except asyncio.TimeoutError:
            return RESULT_ERROR, values, "timeout"
        except Exception as exc:  # noqa: BLE001 - el servidor puede variar
            return RESULT_ERROR, values, f"{type(exc).__name__}: {exc}"

        code = _status_value(result)
        if code != 0:
            if _status_allowed(code, _NO_DATA_CODES):
                return (RESULT_OK, values, None) if values else (RESULT_NO_DATA, 0, None)
            if _status_allowed(code, _UNSUPPORTED_CODES):
                return RESULT_UNSUPPORTED, values, f"status=0x{code:08X}"
            return RESULT_ERROR, values, f"status=0x{code:08X}"

        history = getattr(result, "HistoryData", None)
        data_values = list(getattr(history, "DataValues", []) or [])
        for data_value in data_values:
            record = datavalue_to_record(
                data_value, tag_id=tag_id, connector_id=connector_id
            )
            if record is None:
                continue
            enqueue_or_spill(queue, record, spill)
            values += 1

        pages += 1
        continuation = getattr(result, "ContinuationPoint", None)
        if not continuation:
            break
        if pages >= _MAX_PAGES:
            return RESULT_ERROR, values, "too_many_pages"
        await _breathe(queue)

    return (RESULT_OK, values, None) if values else (RESULT_NO_DATA, 0, None)


async def recover_gaps(
    *,
    tags: Sequence,  # noqa: ANN001
    watermarks: Mapping[int, datetime],
    cutoff: datetime,
    connector_id: str,
    queue,  # noqa: ANN001
    spill: Optional[SpillBuffer],
    max_gap_s: float,
    batch_size: int,
    timeout_s: float,
) -> List[GapResult]:
    """Recupera el hueco de cada tag con watermark conocido.

    No lanza por errores de un tag: los clasifica y los contabiliza. Una
    recuperación nunca debe tumbar la sesión de tiempo real.
    """
    results: List[GapResult] = []
    for tag in tags:
        last_ts = watermarks.get(tag.tag_id)
        if last_ts is None:
            continue
        gap_s = (cutoff - last_ts).total_seconds()
        if gap_s <= 0:
            continue

        try:
            metrics.HISTORY_GAP_SECONDS.observe(gap_s)
        except Exception:  # noqa: BLE001
            pass

        if gap_s > max_gap_s:
            result = RESULT_GAP_TOO_LARGE
            values = 0
            error = f"gap_s={gap_s:.1f} > max_gap_s={max_gap_s:.1f}"
        else:
            started = time.perf_counter()
            try:
                result, values, error = await _read_and_ingest(
                    tag.node,
                    tag_id=tag.tag_id,
                    connector_id=connector_id,
                    start=last_ts,
                    end=cutoff,
                    batch_size=batch_size,
                    timeout_s=timeout_s,
                    queue=queue,
                    spill=spill,
                )
            except Exception as exc:  # noqa: BLE001 - aislamiento por tag
                result, values, error = (
                    RESULT_ERROR, 0, f"{type(exc).__name__}: {exc}",
                )  # type: ignore[assignment]
            finally:
                try:
                    metrics.HISTORY_READ_DURATION.observe(
                        time.perf_counter() - started
                    )
                except Exception:  # noqa: BLE001
                    pass

        try:
            metrics.HISTORY_READ.labels(result=result).inc()
        except Exception:  # noqa: BLE001
            pass
        if values:
            try:
                metrics.HISTORY_READ_VALUES.inc(values)
            except Exception:  # noqa: BLE001
                pass
        if result != RESULT_OK:
            try:
                metrics.HISTORY_GAP_UNRECOVERED.inc()
            except Exception:  # noqa: BLE001
                pass

        gap = GapResult(
            tag_id=tag.tag_id,
            result=result,
            last_ts=last_ts,
            cutoff=cutoff,
            gap_s=gap_s,
            values=values,
            error=error,
        )
        results.append(gap)
        log_fn = log.info if result == RESULT_OK else log.warning
        log_fn(
            "history_gap",
            tag_id=tag.tag_id,
            result=result,
            last_ts=last_ts.isoformat(),
            cutoff=cutoff.isoformat(),
            gap_s=round(gap_s, 3),
            values=values,
            error=error,
        )
        await _breathe(queue)
    return results
