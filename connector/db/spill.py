"""Buffer de *spill* a disco para no perder datos en caídas largas de la BD.

Cuando la cola en memoria (``OPC_QUEUE_MAX_SIZE``) se llena, los registros se vuelcan
a ficheros segmentados en disco (JSON Lines) en lugar de descartarse. El ``BatchWriter``
los relee y reinyecta cuando la base de datos se recupera. Los datos sobreviven a
reinicios del contenedor si el directorio de spill está en un volumen persistente.

Diseño con hilo dedicado (P0-4 rediseño):
- ``write()`` (llamado desde ``datachange_notification`` en el event loop) NUNCA
  toca disco ni adquiere locks bloqueantes: solo ``put_nowait`` en una
  ``queue.Queue`` interna. Coste O(1), sin syscalls.
- Un único hilo worker (``threading.Thread`` daemon) es el dueño exclusivo de
  todos los ficheros: batch-write, fsync por lotes, rotación, cap, métricas y
  replay. No hay ``threading.Lock`` compartido con el loop → el drain ya no
  puede congelar el loop aunque reescriba un segmento de 64 MB.
- ``drain_async()`` coordina sin bloquear: pone una petición en la cola de
  control y el worker responde vía ``loop.call_soon_threadsafe``.
- ``drain()`` síncrono (tests / contextos sin loop) espera con
  ``threading.Event`` con timeout.
- Permisos seguros: dir 0700, segmentos 0600. Serialización orjson con fallback.

Límites:
- ``POSTGRES_SPILL_MAX_MB`` / ``POSTGRES_SPILL_SEGMENT_MB`` / ``POSTGRES_SPILL_FSYNC_EVERY``.
- Cola interna acotada (``_PENDING_MAX``): si se llena, ``write()`` devuelve
  False y el llamador aplica drop-oldest (último recurso, no bloquear el loop).
"""

from __future__ import annotations

import asyncio
import os
import queue
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Tuple

from ..config import DbConfig
from ..utils import metrics
from ..utils.logger import get_logger

log = get_logger(__name__)

_SEG_PREFIX = "spill-"
_SEG_SUFFIX = ".jsonl"

Record = Tuple[int, datetime, Optional[float], Optional[str], int, str]

# ── Serialización rápida (orjson con fallback) ─────────────────────────────
try:  # pragma: no cover - depende del entorno
    import orjson as _orjson

    def _dumps(obj: list) -> bytes:
        return _orjson.dumps(obj) + b"\n"

    def _loads(line: bytes | str) -> list:
        return _orjson.loads(line)

    _SERIALIZER = "orjson"
except ImportError:  # pragma: no cover
    import json as _json

    def _dumps(obj: list) -> bytes:
        return (_json.dumps(obj, separators=(",", ":")) + "\n").encode("utf-8")

    def _loads(line: bytes | str) -> list:
        if isinstance(line, bytes):
            line = line.decode("utf-8")
        return _json.loads(line)

    _SERIALIZER = "json"


def _encode(r: Record) -> list:
    return [r[0], r[1].isoformat(), r[2], r[3], r[4], r[5]]


def _decode(a: list) -> Record:
    return (a[0], datetime.fromisoformat(a[1]), a[2], a[3], a[4], a[5])


# Cola interna acotada: bound de memoria si la BD está caída mucho tiempo.
# 100k registros ≈ 15-30 MB en memoria. Llenarla devuelve False (drop-oldest aguas arriba).
_PENDING_MAX = 100_000
# Lote máximo por write syscall del worker (throughput vs latencia de replay).
_WRITE_BATCH = 1_000
# Timeout de drain síncrono (tests) y async (writer).
_DRAIN_TIMEOUT_S = 15.0


@dataclass
class _DrainReq:
    max_n: int
    # Solo uno de los dos mecanismos de respuesta está activo:
    sync_event: Optional[threading.Event] = None
    sync_out: Optional[list] = field(default=None, repr=False)
    async_future: Optional[Any] = field(default=None, repr=False)
    async_loop: Optional[Any] = field(default=None, repr=False)


@dataclass
class _FlushReq:
    done: threading.Event = field(default_factory=threading.Event)


class SpillBuffer:
    """Buffer persistente en disco con hilo dedicado.

    Hilo del event loop: solo ``write()`` (put_nowait), ``has_data()`` (lectura
    de contadores atómicos) y ``drain_async()`` (encolar petición + await).
    Todo el I/O vive en ``_worker``.
    """

    def __init__(self, cfg: DbConfig, connector_id: str) -> None:
        self.enabled = bool(getattr(cfg, "spill_enabled", False))
        self.dir = Path(getattr(cfg, "spill_dir", "/tmp/spill")) / connector_id
        self.max_bytes = max(1, int(getattr(cfg, "spill_max_mb", 1024))) * 1024 * 1024
        self.segment_bytes = max(1, int(getattr(cfg, "spill_segment_mb", 64))) * 1024 * 1024
        self.fsync_every = max(1, int(getattr(cfg, "spill_fsync_every", 100)))

        # Estado visible desde el loop (solo lectura; lo actualiza el worker).
        # Lectura/escritura de bool/int es atómica bajo GIL: eventual-consistente.
        self._disk_has_data = False
        self._closed = False

        self._pending: Optional[queue.Queue] = None
        self._requests: Optional[queue.Queue] = None
        self._stop = threading.Event()
        self._worker: Optional[threading.Thread] = None

        # Estado exclusivo del worker (nunca tocar desde el loop).
        self._cur_file = None
        self._cur_path: Optional[Path] = None
        self._cur_size = 0
        self._writes_since_fsync = 0

        if not self.enabled:
            return

        # Permisos del directorio aquí (rápido, una vez en arranque, no en hot-path).
        self.dir.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.dir, 0o700)
        except OSError:
            pass

        self._pending = queue.Queue(maxsize=_PENDING_MAX)
        self._requests = queue.Queue()
        self._worker = threading.Thread(
            target=self._worker_loop,
            name=f"spill-{connector_id}",
            daemon=True,
        )
        self._worker.start()
        log.info("spill_enabled", dir=str(self.dir),
                 max_mb=getattr(cfg, "spill_max_mb", 0),
                 serializer=_SERIALIZER, fsync_every=self.fsync_every,
                 mode="dedicated-thread")

    # ══════════════════════════════════════════════════════════════════════
    # API llamada desde el event loop — NUNCA bloquea, NUNCA hace I/O
    # ══════════════════════════════════════════════════════════════════════
    def write(self, record: Record) -> bool:
        """Encola un registro para persistencia. O(1), sin disco, sin locks.

        Devuelve False si el spill está deshabilitado, cerrado o la cola
        interna está llena (el llamador aplica drop-oldest).
        """
        if not self.enabled or self._closed or self._pending is None:
            return False
        try:
            self._pending.put_nowait(record)
            return True
        except queue.Full:
            return False

    def has_data(self) -> bool:
        """¿Hay datos pendientes (en cola interna o ya en disco)? Sin I/O."""
        if not self.enabled or self._closed:
            # Tras close() los datos en disco siguen existiendo para el próximo
            # arranque; pero el buffer ya no acepta más. Para tests: si está
            # cerrado, responder según disco vía chequeo barato ya cacheado.
            if not self.enabled:
                return False
        if self._pending is not None and self._pending.qsize() > 0:
            return True
        return self._disk_has_data

    async def has_data_async(self) -> bool:
        # Sin I/O: respuesta inmediata, sin thread.
        return self.has_data()

    def drain(self, max_n: int) -> List[Record]:
        """Versión síncrona (tests / sin loop): espera al worker con timeout."""
        if not self.enabled or max_n <= 0 or self._requests is None:
            return []
        req = _DrainReq(max_n=max_n, sync_event=threading.Event(), sync_out=[])
        self._requests.put(req)
        ok = req.sync_event.wait(timeout=_DRAIN_TIMEOUT_S)
        if not ok:
            log.warning("spill_drain_timeout", max_n=max_n, mode="sync")
            return list(req.sync_out or [])
        return list(req.sync_out or [])

    async def drain_async(self, max_n: int) -> List[Record]:
        """Petición de replay sin bloquear el loop (responde el worker)."""
        if not self.enabled or max_n <= 0 or self._requests is None:
            return []
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # Sin loop en este hilo: degradar a drain síncrono.
            return await asyncio.to_thread(self.drain, max_n)
        fut: asyncio.Future = loop.create_future()
        self._requests.put(_DrainReq(max_n=max_n, async_future=fut, async_loop=loop))
        try:
            return await asyncio.wait_for(asyncio.shield(fut), timeout=_DRAIN_TIMEOUT_S)
        except asyncio.TimeoutError:
            log.warning("spill_drain_timeout", max_n=max_n, mode="async")
            if not fut.done():
                fut.cancel()
            return []

    def flush(self) -> None:
        """Pide flush+fsync al worker y espera (apagado/tests). No usar en hot-path."""
        if not self.enabled or self._requests is None:
            return
        req = _FlushReq()
        self._requests.put(req)
        req.done.wait(timeout=_DRAIN_TIMEOUT_S)

    def close(self) -> None:
        """Detiene el worker volcando todo lo pendiente (solo apagado)."""
        if not self.enabled or self._closed:
            return
        self._closed = True
        self._stop.set()
        # Despertar al worker si está en get(timeout).
        if self._requests is not None:
            self._requests.put(_FlushReq())  # flush final; el worker sale igual por _stop
        if self._worker is not None and self._worker.is_alive():
            self._worker.join(timeout=10.0)
            if self._worker.is_alive():
                log.warning("spill_close_timeout", detail="worker no terminó en 10s")

    # ══════════════════════════════════════════════════════════════════════
    # Hilo worker — único dueño de los ficheros
    # ══════════════════════════════════════════════════════════════════════
    def _worker_loop(self) -> None:
        assert self._pending is not None and self._requests is not None
        try:
            self._w_open_new_segment()
        except OSError as exc:
            log.error("spill_open_failed", error=str(exc), dir=str(self.dir))
            return
        self._w_update_metrics()
        self._w_refresh_disk_flag()
        last_housekeeping = time.monotonic()

        while True:
            try:
                # 1) Volcar escrituras pendientes en lote (con timeout corto para
                #    atender peticiones de drain/flush aunque no haya writes).
                batch = self._collect_pending(timeout_s=0.05)
                if batch:
                    self._w_write_batch(batch)

                # 2) Atender peticiones de control (drain/flush). Se sirven DESPUÉS
                #    de volcar lo pendiente → el drain ve los writes anteriores (FIFO).
                self._serve_requests()

                # 3) Housekeeping periódico (cap + métricas, no por registro).
                now = time.monotonic()
                if now - last_housekeeping >= 1.0:
                    last_housekeeping = now
                    self._w_enforce_cap()
                    self._w_update_metrics()
                    self._w_refresh_disk_flag()
            except Exception as exc:  # noqa: BLE001 - el worker nunca debe morir
                log.error("spill_worker_error", error=str(exc),
                          error_type=type(exc).__name__)
                # Evitar busy-loop si el error es persistente.
                time.sleep(0.1)

            if self._stop.is_set() and self._pending.empty() and self._requests.empty():
                break

        # Volcado final + cierre.
        try:
            tail = self._collect_pending(timeout_s=0.0)
            if tail:
                self._w_write_batch(tail)
            self._serve_requests()  # drenar peticiones restantes antes de salir
            if self._cur_file:
                try:
                    self._cur_file.flush()
                    os.fsync(self._cur_file.fileno())
                except OSError:
                    pass
                try:
                    self._cur_file.close()
                except OSError:
                    pass
                self._cur_file = None
            self._w_update_metrics()
            self._w_refresh_disk_flag()
        except Exception as exc:  # noqa: BLE001 - el worker nunca debe morir con excepción
            log.error("spill_worker_shutdown_error", error=str(exc))

    def _collect_pending(self, timeout_s: float) -> List[Record]:
        """Saca hasta _WRITE_BATCH registros sin bloquear más de timeout_s."""
        assert self._pending is not None
        batch: List[Record] = []
        try:
            # Primer elemento: espera breve (permite batching a alta tasa).
            first = self._pending.get(timeout=timeout_s) if timeout_s > 0 else self._pending.get_nowait()
            batch.append(first)
        except queue.Empty:
            return batch
        while len(batch) < _WRITE_BATCH:
            try:
                batch.append(self._pending.get_nowait())
            except queue.Empty:
                break
        return batch

    def _serve_requests(self) -> None:
        assert self._requests is not None
        while True:
            try:
                req = self._requests.get_nowait()
            except queue.Empty:
                return
            try:
                if isinstance(req, _FlushReq):
                    self._w_flush_cur()
                    req.done.set()
                elif isinstance(req, _DrainReq):
                    out = self._w_drain(req.max_n)
                    if req.sync_out is not None and req.sync_event is not None:
                        req.sync_out.extend(out)
                        req.sync_event.set()
                    elif req.async_future is not None and req.async_loop is not None:
                        fut, loop = req.async_future, req.async_loop
                        try:
                            loop.call_soon_threadsafe(fut.set_result, out)
                        except RuntimeError:
                            pass  # loop cerrado durante el apagado
            except Exception as exc:  # noqa: BLE001
                log.warning("spill_request_failed", error=str(exc))
                try:
                    if isinstance(req, _FlushReq):
                        req.done.set()
                    elif isinstance(req, _DrainReq):
                        if req.sync_event is not None:
                            req.sync_event.set()
                        elif req.async_future is not None and req.async_loop is not None:
                            loop = req.async_loop
                            try:
                                loop.call_soon_threadsafe(req.async_future.set_result, [])
                            except RuntimeError:
                                pass
                except Exception:  # noqa: BLE001
                    pass

    # ── Primitivas de fichero (SOLO worker) ─────────────────────────────────
    def _w_open_new_segment(self) -> None:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
        self._cur_path = self.dir / f"{_SEG_PREFIX}{stamp}{_SEG_SUFFIX}"
        fd = os.open(self._cur_path, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
        try:
            os.chmod(self._cur_path, 0o600)
        except OSError:
            pass
        self._cur_file = os.fdopen(fd, "a", encoding="utf-8")
        try:
            self._cur_size = self._cur_path.stat().st_size
        except OSError:
            self._cur_size = 0
        self._writes_since_fsync = 0

    def _w_segments(self) -> List[Path]:
        try:
            return sorted(self.dir.glob(f"{_SEG_PREFIX}*{_SEG_SUFFIX}"))
        except OSError:
            return []

    def _w_update_metrics(self) -> None:
        total = 0
        n = 0
        for p in self._w_segments():
            n += 1
            try:
                if p == self._cur_path:
                    total += self._cur_size  # incluye buffered aún sin flush
                else:
                    total += p.stat().st_size
            except OSError:
                pass
        try:
            metrics.SPILL_BYTES.set(total)
            metrics.SPILL_FILES.set(n)
        except Exception:  # noqa: BLE001
            pass

    def _w_refresh_disk_flag(self) -> None:
        """Cache para has_data(): True si algún segmento tiene tamaño > 0."""
        for p in self._w_segments():
            try:
                if p == self._cur_path:
                    if self._cur_size > 0:
                        self._disk_has_data = True
                        return
                elif p.stat().st_size > 0:
                    self._disk_has_data = True
                    return
            except OSError:
                continue
        self._disk_has_data = False

    def _w_enforce_cap(self) -> None:
        try:
            sizes: dict[Path, int] = {}
            total = 0
            for p in self._w_segments():
                try:
                    s = p.stat().st_size if p != self._cur_path else self._cur_size
                except OSError:
                    continue
                sizes[p] = s
                total += s
            while total > self.max_bytes and len(sizes) > 1:
                oldest = sorted(sizes.keys())[0]
                if oldest == self._cur_path:
                    break
                freed = sizes.pop(oldest, 0)
                try:
                    oldest.unlink()
                except OSError:
                    pass
                total -= freed
                try:
                    metrics.SPILL_DROPPED.inc()
                except Exception:  # noqa: BLE001
                    pass
                log.warning("spill_segment_dropped", segment=str(oldest))
        except Exception:  # noqa: BLE001
            pass

    def _w_rotate(self) -> None:
        if self._cur_file:
            try:
                self._cur_file.flush()
                os.fsync(self._cur_file.fileno())
            except OSError:
                pass
            try:
                self._cur_file.close()
            except OSError:
                pass
        self._w_open_new_segment()

    def _w_flush_cur(self) -> None:
        if self._cur_file:
            try:
                self._cur_file.flush()
                os.fsync(self._cur_file.fileno())
            except OSError:
                pass
            self._writes_since_fsync = 0
        self._w_update_metrics()
        self._w_refresh_disk_flag()

    def _w_write_batch(self, batch: List[Record]) -> None:
        if not batch or self._cur_file is None:
            # Si el fichero se perdió, intentar reabrir una vez.
            try:
                self._w_open_new_segment()
            except OSError:
                return
        if self._cur_size >= self.segment_bytes:
            self._w_rotate()
        try:
            chunks: List[str] = []
            for r in batch:
                try:
                    payload = _dumps(_encode(r))
                except Exception:  # noqa: BLE001 - registro no serializable: saltar
                    continue
                chunks.append(payload.decode("utf-8") if isinstance(payload, bytes) else str(payload))
            if not chunks:
                return
            blob = "".join(chunks)
            self._cur_file.write(blob)
            self._cur_size += len(blob.encode("utf-8"))
            self._writes_since_fsync += len(chunks)
            # Marcar disponibilidad de inmediato (has_data() es cacheado y el
            # refresh periódico es cada 1s; sin esto hay una ventana donde lo
            # pendiente ya se consumió pero el flag aún es False).
            self._disk_has_data = True
            try:
                metrics.SPILL_WRITTEN.inc(len(chunks))
            except Exception:  # noqa: BLE001
                pass
            if self._writes_since_fsync >= self.fsync_every:
                try:
                    self._cur_file.flush()
                    os.fsync(self._cur_file.fileno())
                except OSError:
                    pass
                self._writes_since_fsync = 0
        except OSError:
            return

    def _w_drain(self, max_n: int) -> List[Record]:
        """Replay FIFO por streaming con reemplazo atómico (solo worker)."""
        out: List[Record] = []
        if max_n <= 0:
            return out
        # Asegurar que lo pendiente en el fichero (buffered) sea visible.
        if self._cur_file:
            try:
                self._cur_file.flush()
            except OSError:
                pass
        for seg in self._w_segments():
            if len(out) >= max_n:
                break
            take = max_n - len(out)
            consumed, has_remaining = self._w_consume_prefix(seg, take, out)
            if consumed == 0 and not has_remaining:
                self._w_remove_segment(seg)
                continue
            if has_remaining:
                break
            self._w_remove_segment(seg)
        if out:
            try:
                metrics.SPILL_REPLAYED.inc(len(out))
            except Exception:  # noqa: BLE001
                pass
        self._w_update_metrics()
        self._w_refresh_disk_flag()
        return out

    def _w_consume_prefix(self, seg: Path, take: int, out: List[Record]) -> tuple[int, bool]:
        consumed = 0
        tmp_path: Optional[Path] = None
        tmp_file = None
        has_remaining = False
        try:
            with open(seg, "r", encoding="utf-8") as f:
                for raw in f:
                    if consumed >= take:
                        has_remaining = True
                        fd, tmp_name = tempfile.mkstemp(
                            dir=str(self.dir), prefix=".spill-tmp-", suffix=".jsonl")
                        try:
                            os.chmod(tmp_name, 0o600)
                        except OSError:
                            pass
                        tmp_path = Path(tmp_name)
                        tmp_file = os.fdopen(fd, "w", encoding="utf-8")
                        tmp_file.write(raw)
                        for rest in f:
                            tmp_file.write(rest)
                        tmp_file.close()
                        tmp_file = None
                        break
                    s = raw.strip()
                    if not s:
                        continue
                    try:
                        out.append(_decode(_loads(s)))
                        consumed += 1
                    except Exception:  # noqa: BLE001 - línea corrupta: saltar
                        continue
                else:
                    has_remaining = False
        except FileNotFoundError:
            self._w_close_tmp(tmp_file, tmp_path)
            return 0, False
        except OSError:
            self._w_close_tmp(tmp_file, tmp_path)
            return consumed, False

        if tmp_path is not None:
            self._w_replace_segment(seg, tmp_path)
        return consumed, has_remaining

    @staticmethod
    def _w_close_tmp(tmp_file: Any, tmp_path: Optional[Path]) -> None:
        if tmp_file:
            try:
                tmp_file.close()
            except OSError:
                pass
        if tmp_path:
            try:
                tmp_path.unlink()
            except OSError:
                pass

    def _w_replace_segment(self, seg: Path, tmp_path: Path) -> None:
        reopen = seg == self._cur_path
        if reopen and self._cur_file:
            try:
                self._cur_file.close()
            except OSError:
                pass
            self._cur_file = None
        try:
            os.replace(tmp_path, seg)
            try:
                os.chmod(seg, 0o600)
            except OSError:
                pass
        except OSError:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            if reopen:
                try:
                    self._w_open_new_segment()
                except OSError:
                    pass
            return
        if reopen:
            try:
                fd = os.open(seg, os.O_APPEND | os.O_WRONLY, 0o600)
                self._cur_file = os.fdopen(fd, "a", encoding="utf-8")
                self._cur_size = seg.stat().st_size
                self._writes_since_fsync = 0
            except OSError:
                try:
                    self._w_open_new_segment()
                except OSError:
                    pass

    def _w_remove_segment(self, seg: Path) -> None:
        if seg == self._cur_path and self._cur_file:
            try:
                self._cur_file.close()
            except OSError:
                pass
            self._cur_file = None
            try:
                seg.unlink()
            except OSError:
                pass
            try:
                self._w_open_new_segment()
            except OSError:
                pass
        else:
            try:
                seg.unlink()
            except OSError:
                pass


def enqueue_or_spill(queue: "asyncio.Queue", record: Record, spill: Optional[SpillBuffer]) -> None:
    """Encola un registro; si la cola está llena, vuelca a disco (o drop-oldest si no hay spill).

    Con el rediseño, ``spill.write()`` nunca bloquea el loop: si la cola interna
    del spill también está llena, devuelve False y se aplica drop-oldest.
    """
    try:
        queue.put_nowait(record)
        return
    except asyncio.QueueFull:
        pass

    if spill is not None and spill.write(record):
        return

    # Último recurso sin spill (o spill saturado): descartar el más antiguo.
    try:
        queue.get_nowait()
        queue.put_nowait(record)
    except (asyncio.QueueEmpty, asyncio.QueueFull):
        pass
    try:
        metrics.VALUES_DROPPED.inc()
    except Exception:  # noqa: BLE001
        pass
