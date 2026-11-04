# Monitorización del conector OPC-UA

Stack opcional con **Prometheus + Grafana**, alertas y dashboard provisionados.

## Arranque

```bash
# Desde opcua-postgres/
GRAFANA_ADMIN_PASSWORD='cambiar-esto' \
  docker compose -f docker-compose.monitoring.yml up -d

# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (usuario: admin)
```

El conector publica `/metrics` en los puertos del host:

| Servicio | Puerto host | Target Prometheus |
|---|---|---|
| `connector-01` | `8001` | `host.docker.internal:8001` |
| `connector-02` | `8002` | `host.docker.internal:8002` |

Para escalar, añadir cada conector en
[`prometheus/prometheus.yml`](prometheus/prometheus.yml) y recargar:

```bash
curl -X POST http://localhost:9090/-/reload
```

## Contenido

| Fichero | Uso |
|---|---|
| `prometheus/prometheus.yml` | Scrape de los conectores |
| `prometheus/alerts.yml` | Reglas de alerta por disponibilidad, buffer, rendimiento e integridad |
| `grafana/provisioning/` | Datasource Prometheus y carga automática de dashboards |
| `grafana/dashboards/opcua-connector.json` | Dashboard "OPC-UA Connector" |

## Alertas incluidas

| Alerta | Severidad | Condición |
|---|---|---|
| `OpcConnectorTargetDown` | critical | `up == 0` durante 2 min |
| `OpcConnectorDbDown` | critical | `db_status == 0` durante 1 min |
| `OpcConnectorOpcDown` | warning | `session_status == 0` durante 2 min |
| `OpcConnectorQueueHigh` | warning | cola > 80% de su capacidad |
| `OpcConnectorSpillActive` | warning | spill > 0 durante 10 min |
| `OpcConnectorSpillCapacityHigh` | critical | spill > 80% de `POSTGRES_SPILL_MAX_MB` |
| `OpcConnectorSpillDataLoss` | critical | segmentos de spill descartados |
| `OpcConnectorValuesDropped` | critical | valores perdidos por buffer/spill lleno |
| `OpcConnectorBatchLagHigh` | warning | lag > 30 s |
| `OpcConnectorWriteLatencyHigh` | warning | p95 de escritura > 1 s |
| `OpcConnectorDedupDisabled` | warning | deduplicación desactivada |
| `OpcConnectorDuplicatesHigh` | warning | duplicados deduplicados > 10/s |
| `OpcConnectorDrainTimeouts` | warning | timeouts del worker de spill |
| `OpcConnectorReconnectionsHigh` | warning | reconexiones OPC > 1/s |

Ajustar umbrales y `for` al SLA real antes de producción.

## Métricas de capacidad

El conector expone dos gauges para calcular ocupación relativa:

- `opc_connector_queue_capacity` (`OPC_QUEUE_MAX_SIZE`)
- `opc_connector_spill_capacity_bytes` (`POSTGRES_SPILL_MAX_MB`)

## Notas

- El stack de monitorización es **independiente** del de producción.
- Grafana no se expone con TLS ni autenticación anónima: mantenerlo en una red
  de gestión o detrás de un reverse proxy con TLS.
- El dashboard y las alertas son un punto de partida; validar los umbrales con
  la carga real (`tools/load_client.py`) antes de declararlos operativos.
