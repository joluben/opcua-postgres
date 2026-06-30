"""Tests del filtrado de nodos del browser (sin servidor OPC-UA)."""

from types import SimpleNamespace

from connector.opc.browser import _matches


def _node(ns_index: int, node_id: str):
    nodeid = SimpleNamespace(NamespaceIndex=ns_index, to_string=lambda: node_id)
    return SimpleNamespace(nodeid=nodeid)


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
