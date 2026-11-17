"""Regresiones de los artefactos de observabilidad (Fase 3)."""

import json
import logging
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
MONITORING = ROOT / "monitoring"


def test_grafana_dashboard_is_valid_json_with_panels():
    dashboard = json.loads(
        (MONITORING / "grafana" / "dashboards" / "opcua-connector.json").read_text(
            encoding="utf-8"
        )
    )
    assert dashboard["uid"] == "opcua-connector"
    titles = {p["title"] for p in dashboard["panels"]}
    assert {
        "Conectores UP", "Spill en disco",
        "Integridad: duplicados / replay / ACK",
        "Durabilidad del replay", "Apagado ordenado",
    } <= titles
    for panel in dashboard["panels"]:
        assert panel["targets"], f"panel sin consulta: {panel['title']}"


def test_prometheus_alerts_cover_critical_failures():
    alerts = (MONITORING / "prometheus" / "alerts.yml").read_text(encoding="utf-8")
    for name in (
        "OpcConnectorTargetDown",
        "OpcConnectorDbDown",
        "OpcConnectorSpillCapacityHigh",
        "OpcConnectorSpillDataLoss",
        "OpcConnectorSpillDroppedUnacked",
        "OpcConnectorSpillReplayPending",
        "OpcConnectorShutdownDropped",
        "OpcConnectorShutdownSlow",
        "OpcConnectorValuesDropped",
        "OpcConnectorDedupDisabled",
        "OpcConnectorSpillWriteErrors",
        "OpcConnectorSpillFsyncErrors",
        "OpcConnectorSpillRecordsDropped",
    ):
        assert name in alerts


def test_monitoring_stack_references_provisioning():
    compose = (ROOT / "docker-compose.monitoring.yml").read_text(encoding="utf-8")
    assert "monitoring/prometheus:/etc/prometheus:ro" in compose
    assert "monitoring/grafana/provisioning" in compose
    datasource = (
        MONITORING / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    ).read_text(encoding="utf-8")
    assert "uid: prometheus" in datasource


def test_acceptance_script_exists_with_shebang():
    script = ROOT / "scripts" / "acceptance_pg.sh"
    assert script.is_file()
    assert script.read_text(encoding="utf-8").startswith("#!/usr/bin/env bash")


@pytest.mark.asyncio
async def test_http_server_does_not_log_every_request(caplog):
    """Sin Prometheus, cada HEALTHCHECK (30 s) generaba una línea ``aiohttp.access``.

    El endpoint sigue sirviendo /health y /metrics, pero sin ruido de acceso.
    """
    import aiohttp

    from connector.utils import metrics as metrics_mod

    runner = await metrics_mod.start_http_server(0, metrics_mod.HealthState())
    try:
        port = runner.addresses[0][1]
        with caplog.at_level(logging.INFO):
            async with aiohttp.ClientSession() as session:
                async with session.get(f"http://127.0.0.1:{port}/health") as resp:
                    # 503 = degradado (sin OPC ni BD), pero la petición se sirve.
                    assert resp.status == 503
        access = [r for r in caplog.records if r.name.startswith("aiohttp.access")]
        assert not access, f"access logs no deseados: {[r.getMessage() for r in access]}"
    finally:
        await runner.cleanup()
