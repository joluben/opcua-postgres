"""Buffer de *spill* a disco para no perder datos en caídas largas de la BD.

Cuando la cola en memoria (``OPC_QUEUE_MAX_SIZE``) se llena, los registros se vuelcan
a ficheros segmentados en disco (JSON Lines) en lugar de descartarse. El ``BatchWriter``
los relee y reinyecta cuando la base de datos se recupera. Los datos sobreviven a
reinicios del contenedor si el directorio de spill está en un volumen persistente.

Límites:
- ``POSTGRES_SPILL_MAX_MB``: tope total en disco; al superarlo se descartan los segmentos
  más antiguos (último recurso, registrado en ``opc_connector_spill_dropped_total``).
- ``POSTGRES_SPILL_SEGMENT_MB``: tamaño de rotación de segmento.

Cada registro es la tupla lista para COPY (``received_at`` lo rellena el
DEFAULT NOW() de la BD):
    (tag_id, ts, value_num, value_str, quality, connector_id)
"""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

from ..config import DbConfig
from ..utils import metrics
from ..utils.logger import get_logger

log = get_logger(__name__)

_SEG_PREFIX = "spill-"
_SEG_SUFFIX = ".jsonl"

Record = Tuple[int, datetime, Optional[float], Optional[str], int, str]


def _encode(r: Record) -> list:
    return [r[0], r[1].isoformat(), r[2], r[3], r[4], r[5]]


def _decode(a: list) -> Record:
    return (a[0], datetime.fromisoformat(a[1]), a[2], a[3], a[4], a[5])


class SpillBuffer:
    """Buffer persistente en disco, seguro para acceso desde el event loop (lock interno)."""

    def __init__(self, cfg: DbConfig, connector_id: str) -> None:
        self.enabled = cfg.spill_enabled
        self.dir = Path(cfg.spill_dir) / connector_id
        self.max_bytes = max(1, cfg.spill_max_mb) * 1024 * 1024
        self.segment_bytes = max(1, cfg.spill_segment_mb) * 1024 * 1024
        self._lock = threading.Lock()
        self._cur_file = None
        self._cur_path: Optional[Path] = None
        self._cur_size = 0

        if self.enabled:
            self.dir.mkdir(parents=True, exist_ok=True)
            self._open_new_segment()
            self._update_metrics()
            log.info("spill_enabled", dir=str(self.dir), max_mb=cfg.spill_max_mb)

    # ── Gestión de segmentos ────────────────────────────────────────────────
    def _open_new_segment(self) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        self._cur_path = self.dir / f"{_SEG_PREFIX}{stamp}{_SEG_SUFFIX}"
        self._cur_file = open(self._cur_path, "a", encoding="utf-8")
        self._cur_size = self._cur_path.stat().st_size if self._cur_path.exists() else 0

    def _segments(self) -> List[Path]:
        return sorted(self.dir.glob(f"{_SEG_PREFIX}*{_SEG_SUFFIX}"))

    def _update_metrics(self) -> None:
        segs = self._segments()
        total = 0
        for p in segs:
            try:
                total += p.stat().st_size
            except FileNotFoundError:
                pass
        metrics.SPILL_BYTES.set(total)
        metrics.SPILL_FILES.set(len(segs))

    def _enforce_cap(self) -> None:
        segs = self._segments()
        total = sum(p.stat().st_size for p in segs if p.exists())
        while total > self.max_bytes and len(segs) > 1:
            oldest = segs[0]
            if oldest == self._cur_path:
                break
            try:
                total -= oldest.stat().st_size
                oldest.unlink()
            except FileNotFoundError:
                pass
            metrics.SPILL_DROPPED.inc()
            segs = self._segments()

    def _rotate(self) -> None:
        if self._cur_file:
            self._cur_file.close()
        self._open_new_segment()

    # ── API pública ─────────────────────────────────────────────────────────
    def write(self, record: Record) -> bool:
        """Vuelca un registro a disco. Devuelve False si el spill está deshabilitado."""
        if not self.enabled:
            return False
        line = json.dumps(_encode(record), separators=(",", ":")) + "\n"
        encoded = line.encode("utf-8")
        with self._lock:
            self._enforce_cap()
            if self._cur_size >= self.segment_bytes:
                self._rotate()
            self._cur_file.write(line)
            self._cur_file.flush()
            self._cur_size += len(encoded)
            metrics.SPILL_WRITTEN.inc()
            self._update_metrics()
        return True

    def has_data(self) -> bool:
        if not self.enabled:
            return False
        with self._lock:
            for p in self._segments():
                try:
                    if p.stat().st_size > 0:
                        return True
                except FileNotFoundError:
                    continue
            return False

    def drain(self, max_n: int) -> List[Record]:
        """Relee hasta ``max_n`` registros de los segmentos más antiguos y los elimina."""
        if not self.enabled or max_n <= 0:
            return []
        out: List[Record] = []
        with self._lock:
            for seg in self._segments():
                if len(out) >= max_n:
                    break
                if seg == self._cur_path and self._cur_file:
                    self._cur_file.flush()
                try:
                    with open(seg, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                except FileNotFoundError:
                    continue

                take = max_n - len(out)
                consumed, remaining = lines[:take], lines[take:]
                for ln in consumed:
                    ln = ln.strip()
                    if ln:
                        out.append(_decode(json.loads(ln)))

                if remaining:
                    self._overwrite_segment(seg, remaining)
                    break  # segmento parcialmente consumido: detener
                self._remove_segment(seg)

            if out:
                metrics.SPILL_REPLAYED.inc(len(out))
            self._update_metrics()
        return out

    def _overwrite_segment(self, seg: Path, remaining: List[str]) -> None:
        reopen = seg == self._cur_path
        if reopen and self._cur_file:
            self._cur_file.close()
        with open(seg, "w", encoding="utf-8") as f:
            f.writelines(remaining)
        if reopen:
            self._cur_file = open(seg, "a", encoding="utf-8")
            self._cur_size = seg.stat().st_size

    def _remove_segment(self, seg: Path) -> None:
        if seg == self._cur_path and self._cur_file:
            self._cur_file.close()
            try:
                seg.unlink()
            except FileNotFoundError:
                pass
            self._open_new_segment()
        else:
            try:
                seg.unlink()
            except FileNotFoundError:
                pass

    def close(self) -> None:
        with self._lock:
            if self._cur_file:
                self._cur_file.close()
                self._cur_file = None


def enqueue_or_spill(queue: "asyncio.Queue", record: Record, spill: Optional[SpillBuffer]) -> None:
    """Encola un registro; si la cola está llena, vuelca a disco (o drop-oldest si no hay spill)."""
    try:
        queue.put_nowait(record)
        return
    except asyncio.QueueFull:
        pass

    if spill is not None and spill.write(record):
        return

    # Último recurso sin spill: descartar el más antiguo.
    try:
        queue.get_nowait()
        queue.put_nowait(record)
    except (asyncio.QueueEmpty, asyncio.QueueFull):
        pass
    metrics.VALUES_DROPPED.inc()
