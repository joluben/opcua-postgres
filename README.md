# Conector OPC-UA → TimescaleDB

Conector en Python (asyncio) que se suscribe a señales de un servidor **OPC-UA** por
notificaciones **DataChange**, las almacena en memoria y las persiste por lotes (COPY)
en **TimescaleDB**. Se despliega como contenedor Docker y escala horizontalmente con
múltiples instancias, cada una responsable de una partición de tags.

> **Topología:** la base de datos **NO** forma parte de este `docker-compose`. Se despliega
> y opera en un **servidor independiente**. El conector se conecta a `POSTGRES_HOST` por red.

> **Modos de BD:** `POSTGRES_USE_TIMESCALE=true` (por defecto) usa hypertable + compresión.
> Con `false` funciona sobre **PostgreSQL plano** (sin hypertable ni compresión).
>
> **Durabilidad:** el buffer en memoria hace **spill a disco** (`POSTGRES_SPILL_*`) cuando se
> llena, de modo que **no se pierden datos** ante caídas largas de la BD; se reinyectan al
> recuperarse. El directorio de spill debe estar en un **volumen persistente**. El spill se
> gestiona en un **hilo dedicado** (batching + fsync), de forma que **nunca bloquea el event loop**.

> **Seguridad (fail-closed):** por defecto el conector exige `OPC_SECURITY_MODE=SignAndEncrypt`
> y `POSTGRES_SSL_MODE=verify-full`, y **rechaza el arranque** si falta el certificado/clave.
> Los modos inseguros (`None`, `disable`, `prefer`, `allow`, `require`) solo se aceptan de forma
> explícita y **generan warning**.

Plan técnico completo: [`docs/plan_implementacion_conector_opcua.md`](docs/plan_implementacion_conector_opcua.md).

---

## Arquitectura

```
OPC-UA Server ──(DataChange)──▶ Conector(es) Docker ──(TCP 5432 / SSL)──▶ Servidor de BD
                                  asyncio + asyncua                        TimescaleDB (remoto)
```

- **Host del conector:** ejecuta uno o varios contenedores; sin almacenamiento de series.
- **Host de BD (separado):** TimescaleDB; dimensionado por retención (ver plan §15).

## Estructura del proyecto

```
opcua-postgres/
├── connector/
│   ├── main.py              # Orquestación y reconexión
│   ├── config.py            # Carga/validación fail-closed (+ Docker Secrets *_FILE)
│   ├── opc/
│   │   ├── client.py        # Sesión OPC-UA
│   │   ├── security.py      # Políticas y certificados X.509 (allow-list)
│   │   ├── browser.py       # Descubrimiento + partición vía catálogo
│   │   └── subscription.py  # DataChange → asyncio.Queue (chunks de 1000 + deadband)
│   ├── db/
│   │   ├── pool.py          # Pool asyncpg (SSL, CA vía PGSSLROOTCERT, statement_cache)
│   │   ├── initializer.py   # Creación idempotente de tablas/hypertable + verificación de permisos
│   │   ├── writer.py        # Batch writer multi-COPY + requeue + métrica de lag
│   │   └── spill.py         # Spill a disco con hilo dedicado (batching + fsync)
│   └── utils/
│       ├── logger.py        # structlog (JSON)
│       ├── metrics.py       # Prometheus + /health (aiohttp)
│       └── resilience.py    # Backoff exponencial con jitter
├── scripts/dba_setup.sql    # Aprovisionamiento del servidor de BD (ejecuta el DBA)
├── tests/                   # test_security, test_browser, test_writer, test_spill, test_pool
├── Dockerfile               # Multi-stage, usuario no-root, base fijada por digest
├── docker-compose.yml       # Un conector (BD remota)
├── docker-compose.scale.yml # Varios conectores en paralelo
├── .env.example
├── requirements.txt         # Dependencias directas (rangos acotados)
└── requirements.lock        # Versiones exactas resueltas (pip-compile) para builds reproducibles
```

---

## Prerrequisitos

- Docker + Docker Compose 27.x en el host del conector.
- Un **servidor de BD** accesible por red con **TimescaleDB** instalado (ver más abajo).
- Acceso al servidor OPC-UA (URL, credenciales y, según el modo de seguridad, certificados).

## Puesta en marcha

### 1. Aprovisionar la base de datos remota (DBA, una vez)

En el servidor de BD, ejecutar [`scripts/dba_setup.sql`](scripts/dba_setup.sql):

```bash
psql -h <host-bd> -U postgres -d scada_db -f scripts/dba_setup.sql
```

Esto instala la extensión TimescaleDB y crea el usuario `connector_user` con permisos
mínimos (`INSERT`/`SELECT` en datos; `INSERT`/`SELECT`/`UPDATE` en catálogo). Las tablas
y la hypertable las crea el conector de forma **idempotente** en su primera conexión.

### 2. Configurar el conector

```bash
cp .env.example .env
# Editar .env: OPC_SERVER_URL, POSTGRES_HOST (host remoto), POSTGRES_USER, etc.
```

Contraseña de BD vía **Docker Secret** (recomendado):

```bash
mkdir -p secrets
printf '%s' 'LA_CONTRASEÑA_REAL' > secrets/postgres_password.txt
```

> El conector lee `POSTGRES_PASSWORD_FILE` si está presente (tiene prioridad sobre
> `POSTGRES_PASSWORD`). Las carpetas `secrets/` y `certs/` están en `.gitignore`.

### 3. Certificados OPC-UA (modos `Sign` / `SignAndEncrypt`)

```bash
mkdir -p certs
openssl req -x509 -newkey rsa:2048 \
  -keyout certs/client_key.pem -out certs/client_cert.pem \
  -days 1095 -nodes -subj "/CN=OPCUAConnector/O=MiEmpresa/C=CO"
```

El certificado debe **importarse/aprobarse** en el servidor OPC-UA (Siemens, Rockwell,
Ignition, etc.). Se montan en `/certs` en solo lectura.

### 4. Desplegar

Un conector:

```bash
docker compose up -d --build
```

Varios conectores en paralelo (particiones de tags):

```bash
docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --build
```

---

## Observabilidad

Cada conector expone (puerto host `8001`, `8002`, … según el servicio):

- `GET /metrics` — métricas Prometheus (`opc_connector_*`).
- `GET /health`  — `200` sano / `503` degradado, con `opc_connected` y `db_connected`.

```bash
curl http://localhost:8001/health
curl http://localhost:8001/metrics
```

Métricas clave:

| Métrica | Descripción |
|---|---|
| `opc_connector_values_received_total` / `..._written_total` | Valores recibidos / escritos (comparar para detectar backlog) |
| `opc_connector_queue_size` / `..._capacity` | Tamaño actual y capacidad del buffer en memoria |
| `opc_connector_batch_lag_seconds` | Retraso del lote más antiguo (ts OPC → flush) |
| `opc_connector_write_latency_seconds` | Latencia del COPY por lote |
| `opc_connector_spill_bytes` / `..._files` / `..._capacity_bytes` | Bytes, nº de segmentos y límite del spill en disco |
| `opc_connector_spill_written_total` / `..._replayed_total` | Registros volcados / reinyectados desde spill |
| `opc_connector_spill_dropped_total` / `values_dropped_total` | Pérdida por spill lleno / buffer lleno |
| `opc_connector_spill_acked_total` | Lotes de spill confirmados tras el commit en BD |
| `opc_connector_spill_dropped_unacked_total` | Segmentos descartados por el cap sin ACK (pérdida) |
| `opc_connector_spill_replay_pending` | Registros leídos del spill pendientes de ACK |
| `opc_connector_spill_drain_timeouts_total` | Timeouts del drain (no implican pérdida: se reintenta) |
| `opc_connector_shutdown_seconds` | Duración del apagado ordenado (histograma) |
| `opc_connector_shutdown_dropped_total` | Registros no drenados ni persistidos al apagar |
| `opc_connector_values_duplicates_total` | Filas deduplicadas por `ON CONFLICT DO NOTHING` |
| `opc_connector_dedup_enabled` | Deduplicación activa (1/0) |
| `opc_connector_session_status` / `opc_connector_db_status` | Estado OPC (1/0) y BD (1/0) |

### Prometheus + Grafana

El directorio [`monitoring/`](monitoring/) incluye Prometheus, alertas y un
dashboard Grafana provisionado:

```bash
GRAFANA_ADMIN_PASSWORD='cambiar-esto' \
  docker compose -f docker-compose.monitoring.yml up -d
# Prometheus: http://localhost:9090  ·  Grafana: http://localhost:3000
```

Las alertas cubren conectividad, cola, spill, lag, latencia, pérdidas,
deduplicación y reconexiones. Ver [`monitoring/README.md`](monitoring/README.md).

## Tests

```bash
pip install -r requirements.lock pytest
pytest -q
```

50 tests que **no requieren** servidor OPC-UA ni BD: validan la configuración fail-closed,
las políticas SSL (`_build_ssl` y CA vía `PGSSLROOTCERT`), los filtros de browse, el
roundtrip/ACK/reinicio del spill, la compatibilidad con líneas legacy de 6 campos, la
inserción idempotente (`ON CONFLICT`) y la política de requeue del writer.

Prueba de aceptación con PostgreSQL/TimescaleDB real y rol restringido
(`connector_user` sin superusuario):

```bash
make acceptance
# equivale a: ./scripts/acceptance_pg.sh
```

Requiere Docker. Levanta un contenedor efímero, aplica `scripts/dba_setup.sql`,
ejecuta `initialize_schema` como `connector_user` y valida que el replay tras un
crash no duplica filas. En CI se ejecuta en el job `integration`.

---

## Runbook (operación con BD remota)

### Escalado / particionamiento
- Cada conector toma su rango con `OPC_TAG_OFFSET` / `OPC_TAG_LIMIT` sobre el **orden
  estable del catálogo** (`ORDER BY node_id`). Ajustar el nº de conectores al throughput
  **real** de `asyncua` validado en el *spike* (plan §9.3/§14), no al teórico.
- El descubrimiento es *fail-closed*: si hay más variables que las seleccionadas y
  no se configuró `OPC_NODE_ID_FILTER`, `OPC_NAMESPACE_INDEX` ni
  `OPC_ALLOW_TAG_OMISSION=true`, la sesión aborta en vez de dejar tags fuera en
  silencio. Definir `OPC_TAG_OFFSET`/`OPC_TAG_LIMIT` **no** habilita el recorte:
  solo el compose de escalado activa `OPC_ALLOW_TAG_OMISSION=true`. Los errores
  de browse se registran por nodo (`opc_browse_error`) y se cuentan en
  `opc_connector_browse_errors_total`.
- Antes de escalar, verificar en el servidor OPC-UA: `MaxSessionCount` y
  `MaxMonitoredItemsPerSubscription`, y el licenciamiento por sesión.

### Diagnóstico por síntoma

| Síntoma | Causa probable | Acción |
|---|---|---|
| `/health` 503 con `db_connected=false` | BD remota o red caída | Revisar `POSTGRES_HOST`/firewall/SSL; el buffer absorbe hasta `OPC_QUEUE_MAX_SIZE`; al recuperar, se vacía solo |
| `opc_connector_spill_bytes` crece | BD caída: el buffer se está volcando a disco | Normal y esperado; los datos se reinyectan al recuperar la BD. Vigilar espacio en disco del volumen de spill |
| `opc_connector_spill_dropped_total` o `opc_connector_values_dropped_total` crecen | Spill lleno (`POSTGRES_SPILL_MAX_MB`) o spill deshabilitado | Ampliar `POSTGRES_SPILL_MAX_MB`/disco, o `OPC_QUEUE_MAX_SIZE`; definir SLA de pérdida |
| `/health` 503 con `opc_connected=false` | Sesión OPC-UA perdida | El conector reconecta con backoff; revisar red/`MaxSessionCount` |
| Arranque falla: *extensión TimescaleDB no instalada* | DBA no ejecutó `dba_setup.sql` | Ejecutar el script en la BD remota |
| Arranque falla: *permiso insuficiente* | Falta `INSERT`/`UPDATE` | Revisar `GRANT` (§8.2 del plan / `dba_setup.sql`) |
| Arranque falla: *permission denied for schema public* | Falta `CREATE` en el schema (PostgreSQL 15+) | Ejecutar `GRANT USAGE, CREATE ON SCHEMA public TO connector_user;` |
| Tags duplicados/huecos entre conectores | Particionamiento no coordinado | Asegurar partición por catálogo (`node_id`), no por browse ad-hoc |
| Latencia de escritura alta | Chunks grandes / BD subdimensionada | Revisar tuning (§15.2), chunk de 15 min, recursos del host de BD |

### Apagado ordenado

1. Docker envía `SIGTERM`; el conector activa `stop_event`, desconecta OPC y
   detiene los writers.
2. El lote local intenta un último insert; el resto de la cola se persiste en el
   spill. Después se cierra el pool.
3. El proceso debe terminar dentro de `SHUTDOWN_TIMEOUT_S` (30 s por defecto),
   menor que `stop_grace_period` del contenedor (45 s).
4. Verificar en logs `shutdown_requested` → `connector_stop` y que no aparecen
   `spill_close_timeout` ni `max_retries_reached`.

```bash
docker compose stop connector-01
docker compose logs --tail=50 connector-01
```

Si el contenedor recibe `SIGKILL`, el spill no confirmado se reintenta al
arrancar; la clave `event_id` evita duplicados.

### Rollback de versión

El conector anterior a la Fase 2 **no entiende** las líneas de spill de 7 campos
(con `event_id`). Antes de bajar de versión:

```bash
# 1. Arrancar la versión nueva con la BD sana y esperar a que el spill drene:
watch -n2 'curl -s http://localhost:8001/metrics | grep opc_connector_spill_bytes'
# opc_connector_spill_bytes debe llegar a 0.

# 2. Parar el conector.
docker compose stop connector-01

# 3. Verificar que no quedan segmentos pendientes:
docker compose run --rm --entrypoint sh connector-01 \
  -c 'ls -la /var/lib/connector/spill/*/*.jsonl 2>/dev/null || echo "spill vacío"'
```

Si no se puede drenar, exportar/reprocesar el spill con la versión nueva antes
de desplegar la antigua; no borrar segmentos sin aceptar pérdida de datos.
La migración de schema (`event_id` + índice) es compatible hacia atrás: la
versión antigua inserta `event_id=NULL`. Si se desactiva
`POSTGRES_DEDUP_ENABLED`, el índice no se crea en arranques nuevos.

### Mantenimiento
- **Certificados OPC-UA:** validez recomendada ≤ 3 años. Alertar 30 días antes del vencimiento.
- **Retención/compresión:** compresión automática > 1 mes; retención automática de 365 días (`add_retention_policy`)
  según necesidad. Dimensionar disco del host de BD por retención (plan §15.3).
- **Reinicios:** `restart: unless-stopped`; tras agotar `RECONNECT_MAX_RETRIES`, el contenedor
  termina y Docker lo reinicia.

### Seguridad
- `.env`, `secrets/` y `certs/` nunca se commitean.
- **Fail-closed por defecto:** `OPC_SECURITY_MODE=SignAndEncrypt` y `POSTGRES_SSL_MODE=verify-full`.
  Con modo seguro, el conector **no arranca** si faltan `OPC_CERTIFICATE_PATH`/`OPC_PRIVATE_KEY_PATH`.
- BD con `POSTGRES_SSL_MODE=verify-full` + CA montada (`PGSSLROOTCERT`, en el compose `/certs/ca.pem`).
  `require`/`prefer`/`allow`/`disable` cifran sin verificar el servidor y **generan warning**.
- Políticas OPC-UA admitidas: `Basic256Sha256`, `Aes128_Sha256_RsaOaep`, `Aes256_Sha256_RsaPss`
  (se rechazan las obsoletas `Basic128Rsa15`/`Basic256`).
- Contenedor sin root; usuario de BD con permisos mínimos; spill en disco con permisos `0700`/`0600`.
- Nunca loggear variables de entorno completas ni valores de proceso.
