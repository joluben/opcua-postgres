"""Tests del filtrado de nodos del browser (sin servidor OPC-UA)."""

import asyncio
from types import SimpleNamespace

import pytest
from asyncua import ua

from connector.opc.browser import (
    DiscoveryError,
    _collect_variables,
    _matches,
    _select_partition,
)


def _node(ns_index: int, node_id: str):
    nodeid = SimpleNamespace(NamespaceIndex=ns_index, to_string=lambda: node_id)
    return SimpleNamespace(nodeid=nodeid)


def _cfg(**overrides):
    values = dict(
        namespace_index=None,
        node_id_filter=None,
        tag_offset=0,
        tag_limit=5000,
        allow_tag_omission=False,
    )
    values.update(overrides)
    return SimpleNamespace(**values)


def test_matches_without_filters():
    cfg = SimpleNamespace(namespace_index=None, node_id_filter=None)
    assert _matches(_node(2, "ns=2;s=Planta1.Temp1"), cfg) is True


def test_matches_namespace_filter():
    cfg = SimpleNamespace(namespace_index=2, node_id_filter=None)
    assert _matches(_node(2, "ns=2;s=A"), cfg) is True
    assert _matches(_node(3, "ns=3;s=A"), cfg) is False


def test_matches_glob_filter():
    cfg = SimpleNamespace(namespace_index=None, node_id_filter="ns=2;s=Planta1.*")
    assert _matches(_node(2, "ns=2;s=Planta1.Temp1"), cfg) is True
    assert _matches(_node(2, "ns=2;s=Planta2.Temp1"), cfg) is False


def test_partition_fails_when_default_limit_omits_tags():
    variables = [_node(2, f"ns=2;s=Tag{i:04d}") for i in range(6)]

    with pytest.raises(DiscoveryError, match="Descubrimiento incompleto"):
        _select_partition(variables, _cfg(tag_limit=5))


def test_partition_allows_omission_with_namespace_filter():
    variables = [_node(2, f"ns=2;s=Tag{i:04d}") for i in range(6)]

    selected = _select_partition(
        variables, _cfg(tag_limit=5, namespace_index=2)
    )

    assert len(selected) == 5


def test_partition_allows_omission_when_configured_explicitly():
    variables = [_node(2, f"ns=2;s=Tag{i:04d}") for i in range(6)]

    selected = _select_partition(
        variables, _cfg(tag_limit=2, tag_offset=2, allow_tag_omission=True)
    )

    assert [n.nodeid.to_string() for n in selected] == [
        "ns=2;s=Tag0002",
        "ns=2;s=Tag0003",
    ]


def test_partition_without_omission_is_fine():
    variables = [_node(2, f"ns=2;s=Tag{i:04d}") for i in range(3)]

    selected = _select_partition(variables, _cfg(tag_limit=5))

    assert len(selected) == 3


class _FailingNode:
    def __init__(self, node_id: str) -> None:
        self.nodeid = SimpleNamespace(to_string=lambda: node_id)

    async def get_children(self):
        raise ua.UaError("access denied")

    async def read_node_class(self):
        raise ua.UaError("access denied")


class _VariableNode:
    def __init__(self, node_id: str) -> None:
        self.nodeid = SimpleNamespace(to_string=lambda: node_id)

    async def read_node_class(self):
        return ua.NodeClass.Variable


class _RootNode:
    def __init__(self, children) -> None:
        self.nodeid = SimpleNamespace(to_string=lambda: "i=85")
        self._children = children

    async def get_children(self):
        return self._children


class _FakeClient:
    def __init__(self, root) -> None:
        self.nodes = SimpleNamespace(objects=root)


def test_browse_errors_are_counted_and_do_not_hide_other_tags():
    from connector.utils import metrics

    root = _RootNode([
        _VariableNode("ns=2;s=Good"),
        _FailingNode("ns=2;s=Denied"),
    ])
    before = metrics.BROWSE_ERRORS.labels(kind="read_node_class")._value.get()

    found = asyncio.run(_collect_variables(_FakeClient(root), _cfg()))

    assert [n.nodeid.to_string() for n in found] == ["ns=2;s=Good"]
    assert (
        metrics.BROWSE_ERRORS.labels(kind="read_node_class")._value.get() - before
        == 1
    )


def test_browse_get_children_error_is_counted():
    from connector.utils import metrics

    root = _FailingNode("i=85")
    before = metrics.BROWSE_ERRORS.labels(kind="get_children")._value.get()

    found = asyncio.run(_collect_variables(_FakeClient(root), _cfg()))

    assert found == []
    assert (
        metrics.BROWSE_ERRORS.labels(kind="get_children")._value.get() - before
        == 1
    )
