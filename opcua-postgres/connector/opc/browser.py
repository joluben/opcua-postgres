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


@dataclass(slots=True)
class Tag:
    node: Node
    node_id: str
    tag_id: int


async def _collect_variables(client: Client, cfg: OpcConfig) -> List[Node]:
    """Recorre el Address Space desde Objects y devuelve los nodos Variable."""
    found: List[Node] = []
    visited: set[str] = set()
    root = client.nodes.objects

    async def _walk(node: Node, depth: int) -> None:
        if depth > _MAX_DEPTH:
            return
        try:
            children = await node.get_children()
        except ua.UaError:
            return
        for child in children:
            nid = child.nodeid.to_string()
            if nid in visited:
                continue
            visited.add(nid)
            try:
                node_class = await child.read_node_class()
            except ua.UaError:
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


async def _describe(node: Node) -> Dict[str, Optional[str]]:
    display_name = None
    data_type = None
    try:
        dn = await node.read_display_name()
        display_name = dn.Text
    except ua.UaError:
        pass
    try:
        vtype = await node.read_data_type_as_variant_type()
        data_type = vtype.name
    except ua.UaError:
        pass
    return {"display_name": display_name, "data_type": data_type}


async def discover_and_partition(
    client: Client, pool: asyncpg.Pool, opc: OpcConfig, db: DbConfig
) -> List[Tag]:
    """Descubre variables, las persiste en el catálogo y devuelve la partición local."""
    variables = await _collect_variables(client, opc)
    variables.sort(key=lambda n: n.nodeid.to_string())
    log.info("opc_browse_done", total_variables=len(variables))

    start = opc.tag_offset
    end = opc.tag_offset + opc.tag_limit
    partition = variables[start:end]
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
