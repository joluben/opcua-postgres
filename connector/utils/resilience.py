"""Reconexión con backoff exponencial y jitter.

Evita tormentas de reconexión cuando varias instancias pierden conectividad a la vez.
"""

from __future__ import annotations

import asyncio
import random
from typing import Awaitable, Callable, TypeVar

from .logger import get_logger

T = TypeVar("T")

log = get_logger(__name__)

MAX_DELAY_S = 120.0


def backoff_delay(attempt: int, base_delay_s: float) -> float:
    """Calcula ``min(base * 2^attempt, 120) + jitter``."""
    raw = base_delay_s * (2 ** attempt)
    capped = min(raw, MAX_DELAY_S)
    jitter = random.uniform(0, capped * 0.25)
    return capped + jitter


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    *,
    max_retries: int,
    base_delay_s: float,
    description: str,
    on_retry: Callable[[], None] | None = None,
) -> T:
    """Ejecuta ``operation`` reintentando con backoff exponencial.

    ``max_retries < 0`` significa reintentos indefinidos.
    Lanza la última excepción si se agotan los reintentos.
    """
    attempt = 0
    while True:
        try:
            return await operation()
        except Exception as exc:  # noqa: BLE001
            if 0 <= max_retries <= attempt:
                log.error("retry_exhausted", op=description, attempts=attempt, error=str(exc))
                raise
            delay = backoff_delay(attempt, base_delay_s)
            log.warning("retry", op=description, attempt=attempt, delay_s=round(delay, 2), error=str(exc))
            if on_retry is not None:
                on_retry()
            await asyncio.sleep(delay)
            attempt += 1
