"""Representación del StatusCode OPC-UA en la columna ``quality``.

OPC UA define el StatusCode como **uint32** (0 … 4.294.967.295) mientras que la
columna ``quality`` es ``INTEGER`` de PostgreSQL (**int32 con signo**, máximo
2.147.483.647). Los códigos *Bad* empiezan en ``0x80000000``, así que un único
valor con calidad *Bad* desbordaba la columna y hacía fallar el ``COPY`` del lote
completo (``OverflowError: value out of int32 range``).

Se almacena el mismo patrón de 32 bits interpretado con signo, que es la
representación que usan las herramientas OPC-UA (.NET muestra ``0xC0000000``
como ``-1073741824``). No se pierde información y el código sin signo se
recupera en SQL con ``quality::bigint & 4294967295``.
"""

from __future__ import annotations

_UINT32_MASK = 0xFFFFFFFF
_INT32_MAX = 0x7FFFFFFF


def to_quality(status: int | None) -> int:
    """Convierte un StatusCode OPC-UA (uint32) al valor de la columna ``quality``.

    Es idempotente: aplicarlo a un valor ya convertido lo deja igual, de forma
    que sirve tanto para los valores que llegan del servidor como para los que
    se leen de un spill escrito por una versión anterior.
    """
    code = int(status or 0) & _UINT32_MASK
    return code - (_UINT32_MASK + 1) if code > _INT32_MAX else code
