"""Descubrimiento de tags en el Address Space y poblado del catálogo.

Particionamiento (§11.1): el catálogo es la **fuente de verdad**. Se descubren las
variables, se ordenan de forma **estable por node_id** y cada conector selecciona su
partición con ``OFFSET``/``LIMIT`` sobre ese orden, evitando solapes/huecos entre
instancias aunque el Address Space cambie de orden entre browses.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass
from typing import Dict, List, Optional

import asyncpg
from asyncua import Client, Node, ua

from ..config import DbConfig, OpcConfig
from ..utils.logger import get_logger
from ..utils import metrics

log = get_logger(__name__)

_MAX_DEPTH = 25


class DiscoveryError(RuntimeError):
    """Descubrimiento potencialmente incompleto (fail-closed)."""


@dataclass(slots=True)
class Tag:
    node: Node
    node_id: str
    tag_id: int


def _record_browse_error(kind: str, node_id: str, exc: BaseException) -> None:
    """Registra un error de browse sin interrumpir el resto del recorrido."""
    try:
        metrics.BROWSE_ERRORS.labels(kind=kind).inc()
    except Exception:  # noqa: BLE001
        pass
    log.warning(
        "opc_browse_error",
        kind=kind,
        node_id=node_id,
        error=str(exc),
        error_type=type(exc).__name__,
    )


async def _collect_variables(client: Client, cfg: OpcConfig) -> List[Node]:
    """Recorre el Address Space desde Objects y devuelve los nodos Variable.

    Los errores de browse no abortan el recorrido, pero se registran con el
    nodo y la causa: un fallo de permisos o de navegación no debe pasar
    desapercibido.
    """
    found: List[Node] = []
    visited: set[str] = set()
    root = client.nodes.objects

    async def _walk(node: Node, depth: int) -> None:
        nid = node.nodeid.to_string()
        if depth > _MAX_DEPTH:
            _record_browse_error(
                "max_depth", nid, RuntimeError(f"profundidad > {_MAX_DEPTH}")
            )
            return
        try:
            children = await node.get_children()
        except ua.UaError as exc:
            _record_browse_error("get_children", nid, exc)
            return
        for child in children:
            child_id = child.nodeid.to_string()
            if child_id in visited:
                continue
            visited.add(child_id)
            try:
                node_class = await child.read_node_class()
            except ua.UaError as exc:
                _record_browse_error("read_node_class", child_id, exc)
                continue
            if node_class == ua.NodeClass.Variable:
                if _matches(child, cfg):
                    found.append(child)
            elif node_class == ua.NodeClass.Object:
                await _walk(child, depth + 1)

    await _walk(root, 0)
    return found


def _matches(node: Node, cfg: OpcConfig) -> bool:
    if cfg.namespace_index is not None and node.nodeid.NamespaceIndex != cfg.namespace_index:
        return False
    if cfg.node_id_filter:
        return fnmatch.fnmatch(node.nodeid.to_string(), cfg.node_id_filter)
    return True


def _describe_error(node: Node, field: str, exc: BaseException) -> None:
    try:
        metrics.BROWSE_ERRORS.labels(kind="describe").inc()
    except Exception:  # noqa: BLE001
        pass
    log.warning(
        "opc_describe_error",
        node_id=node.nodeid.to_string(),
        field=field,
        error=str(exc),
        error_type=type(exc).__name__,
    )


async def _describe(node: Node) -> Dict[str, Optional[str]]:
    display_name = None
    data_type = None
    try:
        dn = await node.read_display_name()
        display_name = dn.Text
    except ua.UaError as exc:
        _describe_error(node, "display_name", exc)
    try:
        vtype = await node.read_data_type_as_variant_type()
        data_type = vtype.name
    except ua.UaError as exc:
        _describe_error(node, "data_type", exc)
    return {"display_name": display_name, "data_type": data_type}


def _selection_is_explicit(cfg: OpcConfig) -> bool:
    """True si el operador acotó el descubrimiento a propósito.

    No se infiere de ``OPC_TAG_OFFSET``/``OPC_TAG_LIMIT``: el despliegue base
    los define y eso anularía la protección. El recorte por límite solo se
    permite con ``OPC_ALLOW_TAG_OMISSION=true`` (pensado para escalado).
    """
    return bool(
        cfg.node_id_filter
        or cfg.namespace_index is not None
        or getattr(cfg, "allow_tag_omission", False)
    )


def _select_partition(variables: List[Node], cfg: OpcConfig) -> List[Node]:
    """Aplica offset/limit y falla si se descartan tags sin criterio explícito.

    El valor por defecto de ``OPC_TAG_LIMIT`` (5000) no debe truncar el catálogo
    en silencio: si hay más variables de las seleccionadas y no hay filtro por
    nodo/namespace ni ``OPC_ALLOW_TAG_OMISSION=true``, se aborta el
    descubrimiento con un mensaje accionable.
    """
    start = cfg.tag_offset
    end = cfg.tag_offset + cfg.tag_limit
    partition = variables[start:end]
    omitted = len(variables) - len(partition)
    if omitted > 0:
        if not _selection_is_explicit(cfg):
            raise DiscoveryError(
                f"Descubrimiento incompleto: {len(variables)} variables encontradas y "
                f"solo {len(partition)} seleccionadas (OPC_TAG_OFFSET={start}, "
                f"OPC_TAG_LIMIT={cfg.tag_limit}). Configure OPC_NODE_ID_FILTER, "
                "OPC_NAMESPACE_INDEX o, si el recorte es intencionado, "
                "OPC_ALLOW_TAG_OMISSION=true."
            )
        # Partición explícita: no se aborta, pero el recorte no debe ser mudo.
        log.warning(
            "opc_partition_omits_tags",
            total_variables=len(variables),
            selected=len(partition),
            omitted=omitted,
            offset=start,
            limit=cfg.tag_limit,
        )
    return partition


async def discover_and_partition(
    client: Client, pool: asyncpg.Pool, opc: OpcConfig, db: DbConfig
) -> List[Tag]:
    """Descubre variables, las persiste en el catálogo y devuelve la partición local."""
    variables = await _collect_variables(client, opc)
    variables.sort(key=lambda n: n.nodeid.to_string())
    log.info("opc_browse_done", total_variables=len(variables))

    partition = _select_partition(variables, opc)
    start = opc.tag_offset
    log.info("opc_partition", offset=start, limit=opc.tag_limit, assigned=len(partition))

    tags: List[Tag] = []
    async with pool.acquire() as conn:
        for node in partition:
            node_id = node.nodeid.to_string()
            meta = await _describe(node)
            tag_id = await conn.fetchval(
                f"""
                INSERT INTO {db.catalog_table} (node_id, display_name, data_type, active, updated_at)
                VALUES ($1, $2, $3, TRUE, NOW())
                ON CONFLICT (node_id) DO UPDATE
                    SET display_name = EXCLUDED.display_name,
                        data_type    = EXCLUDED.data_type,
                        active       = TRUE,
                        updated_at   = NOW()
                RETURNING id;
                """,
                node_id,
                meta["display_name"],
                meta["data_type"],
            )
            tags.append(Tag(node=node, node_id=node_id, tag_id=tag_id))

    metrics.TAGS_TOTAL.set(len(tags))
    return tags
