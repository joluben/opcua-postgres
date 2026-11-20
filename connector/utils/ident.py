"""Validación y cualificación de identificadores SQL (schema y tablas).

El schema y los nombres de tabla provienen de variables de entorno y se
interpolan en SQL: se validan contra una allow-list estricta y siempre se citan
con comillas dobles, de modo que un identificador aceptado nunca pueda inyectar
SQL (y un nombre con mayúsculas se resuelva tal cual, sin plegado a minúsculas).
"""

from __future__ import annotations

import re

_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class IdentifierError(ValueError):
    """Identificador SQL (schema o tabla) inválido."""


def validate_ident(name: str) -> str:
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise IdentifierError(
            f"Identificador SQL inválido: {name!r}. "
            "Formato permitido: [A-Za-z_][A-Za-z0-9_]*."
        )
    return name


def quote_ident(name: str) -> str:
    """Devuelve el identificador entre comillas dobles, ya validado."""
    return f'"{validate_ident(name)}"'


def qualified_name(schema: str, table: str) -> str:
    """``"schema"."tabla"`` listo para interpolar en una sentencia SQL."""
    return f"{quote_ident(schema)}.{quote_ident(table)}"
