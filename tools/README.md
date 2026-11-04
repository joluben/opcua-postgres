# Herramientas de prueba (Fase 5)

Utilidades para validar el conector sin un servidor OPC-UA físico:

- **`opcua_sim_server.py`** — servidor OPC-UA simulado con N tags y tasa de cambios configurable.
- **`load_client.py`** — *spike* que mide el throughput de deserialización de `asyncua` (sin BD).

Requisitos: `pip install -r requirements.txt` (ya incluye `asyncua`).

---

## 1. Spike de throughput de `asyncua` (§14)

Mide cuántas notificaciones/s puede procesar `asyncua` en Python. Es el dato que define
cuántos conectores se necesitan. **No usa base de datos.**

Terminal 1 — servidor simulado (10k tags, 20k cambios/s):

```powershell
python tools/opcua_sim_server.py --tags 10000 --changes-per-sec 20000
```

Terminal 2 — cliente de medición (60 s):

```powershell
python tools/load_client.py --duration 60
```

Salida esperada (ejemplo):

```
[load]     19,980 val/s | lat ms p50=  45.2 p95= 120.4 avg= 52.1 | total=99,900
[load] RESUMEN: pico=20,100 val/s, total=1,200,300 en 60s
```

**Interpretación:** si un conector estabiliza ~X val/s sin que la latencia crezca sin límite,
entonces `nº_conectores ≈ techo(carga_total_objetivo / X)`. Repetir subiendo
`--changes-per-sec` hasta encontrar el punto donde la latencia se dispara (saturación).

### Parámetros útiles

| Script | Opción | Descripción |
|---|---|---|
| sim | `--tags` | Nº de variables expuestas |
| sim | `--changes-per-sec` | Tasa objetivo de cambios (0 = todos los tags cada ciclo) |
| sim | `--cycle-ms` | Periodo del bucle de actualización (def. 100) |
| load | `--publish-interval-ms` | Intervalo de publicación de la suscripción |
| load | `--queue-size` | QueueSize del MonitoredItem en el servidor |
| load | `--max-tags` | Limitar tags suscritos |
| load | `--duration` | Duración de la prueba (0 = infinito) |

---

## 2. Pipeline completo (conector → TimescaleDB)

### Opción rápida: `docker-compose.test.yml` (recomendada)

Levanta TimescaleDB + simulador + conector en un solo comando (autocontenido):

```powershell
docker compose -f docker-compose.test.yml up --build
```

- Conector: `http://localhost:8000/health` y `http://localhost:8000/metrics`
- BD de pruebas expuesta en `localhost:5432` (`scada_db` / `connector_user` / `test`)
- Ajusta carga editando `--tags` / `--changes-per-sec` del servicio `opcua-sim`.

Para verificar datos:

```powershell
docker compose -f docker-compose.test.yml exec timescaledb psql -U connector_user -d scada_db -c "SELECT count(*) FROM opc_raw_values;"
```

Probar spill/resiliencia: `docker compose -f docker-compose.test.yml stop timescaledb`
(observa `opc_connector_spill_bytes` crecer) y luego `start timescaledb` (reinyección).

### Opción manual (paso a paso)

**a) TimescaleDB local de pruebas** (efímero; la BD de producción es remota):

```powershell
docker run -d --name ts-test -p 5432:5432 `
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=scada_db -e POSTGRES_USER=connector_user `
  timescale/timescaledb:latest-pg16
```

**b) Servidor simulado** (sin seguridad):

```powershell
python tools/opcua_sim_server.py --tags 5000 --changes-per-sec 10000
```

**c) Conector** apuntando al simulador y a la BD local. Crea un `.env` de prueba:

```ini
OPC_SERVER_URL=opc.tcp://localhost:4840/freeopcua/server/
OPC_SECURITY_MODE=None
OPC_NAMESPACE_INDEX=2
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=scada_db
POSTGRES_USER=connector_user
POSTGRES_PASSWORD=test
POSTGRES_SSL_MODE=disable
CONNECTOR_ID=connector-test
OPC_TAG_OFFSET=0
OPC_TAG_LIMIT=5000
LOG_FORMAT=pretty
```

```powershell
python -m connector.main
```

**d) Observabilidad:** `http://localhost:8000/health` y `http://localhost:8000/metrics`
(`opc_connector_values_received_total`, `..._values_written_total`, `..._write_latency_seconds`).

**e) Verificar en BD:**

```sql
SELECT count(*) FROM opc_raw_values;
SELECT * FROM opc_raw_values ORDER BY ts DESC LIMIT 5;
```

### Probar resiliencia / spill
- Detén la BD (`docker stop ts-test`) durante la ingesta: observa `opc_connector_spill_bytes` crecer.
- Reanúdala (`docker start ts-test`): los datos se reinyectan (`opc_connector_spill_replayed_total`).

> Nota: el simulador escribe valores de forma secuencial; su propio techo de `writes/s` puede
> ser el cuello de botella antes que el cliente. Compara los `writes/s reales` del simulador
> con los `val/s` del cliente para distinguir qué componente satura.
