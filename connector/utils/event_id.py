"""Identificador de evento determinista para deduplicación.

El replay del spill y la recuperación de huecos son at-least-once. Para que
PostgreSQL pueda absorber reentregas con ``ON CONFLICT (event_id, ts)``, el
``event_id`` debe ser estable para el mismo dato físico. Se deriva del contenido
(tag, timestamp, valor y calidad), no del momento de la ingesta.

Se usa la misma función para:
- ingesta en tiempo real,
- recuperación histórica por ``HistoryRead``,
- líneas de spill antiguas sin ``event_id`` (compatibilidad).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

# Namespace estable: cambiarlo invalidaría la deduplicación de spills previos.
_NAMESPACE = uuid.UUID("6f0c9d5e-3b8a-4c1d-9f2e-7a1b2c3d4e5f")


def deterministic_event_id(
    tag_id: int,
    ts: datetime,
    value_num: Optional[float],
    value_str: Optional[str],
    quality: int,
    connector_id: str,
) -> uuid.UUID:
    """UUID v5 estable a partir del contenido del registro.

    ``repr`` distingue ``None`` de ``0``/``False`` y mantiene el mismo formato
    que usaba ``spill._legacy_event_id`` antes de centralizarse aquí.
    """
    key = "|".join([
        str(tag_id), ts.isoformat(), repr(value_num), repr(value_str),
        str(quality), str(connector_id),
    ])
    return uuid.uuid5(_NAMESPACE, key)
