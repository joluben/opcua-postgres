"""Regresiones de los artefactos de observabilidad (Fase 3)."""

import json
from pathlib import Path

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
    assert {"Conectores UP", "Spill en disco", "Integridad: duplicados / replay / ACK"} <= titles
    for panel in dashboard["panels"]:
        assert panel["targets"], f"panel sin consulta: {panel['title']}"


def test_prometheus_alerts_cover_critical_failures():
    alerts = (MONITORING / "prometheus" / "alerts.yml").read_text(encoding="utf-8")
    for name in (
        "OpcConnectorTargetDown",
        "OpcConnectorDbDown",
        "OpcConnectorSpillCapacityHigh",
        "OpcConnectorSpillDataLoss",
        "OpcConnectorValuesDropped",
        "OpcConnectorDedupDisabled",
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
