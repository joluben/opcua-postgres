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
./
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
| `opc_connector_queue_size` | Tamaño actual del buffer en memoria |
| `opc_connector_batch_lag_seconds` | Retraso del lote más antiguo (ts OPC → flush) |
| `opc_connector_write_latency_seconds` | Latencia del COPY por lote |
| `opc_connector_spill_bytes` / `..._files` | Bytes y nº de segmentos de spill en disco |
| `opc_connector_spill_written_total` / `..._replayed_total` | Registros volcados / reinyectados desde spill |
| `opc_connector_spill_dropped_total` / `values_dropped_total` | Pérdida por spill lleno / buffer lleno |
| `opc_connector_session_status` / `opc_connector_db_status` | Estado OPC (1/0) y BD (1/0) |

## Tests

```bash
pip install -r requirements.lock pytest
pytest -q
```

21 tests que **no requieren** servidor OPC-UA ni BD: validan la configuración fail-closed,
las políticas SSL (`_build_ssl` y CA vía `PGSSLROOTCERT`), los filtros de browse, el
roundtrip/parcial/drop-oldest del spill y la política de requeue del writer.

---

## Runbook (operación con BD remota)

### Escalado / particionamiento
- Cada conector toma su rango con `OPC_TAG_OFFSET` / `OPC_TAG_LIMIT` sobre el **orden
  estable del catálogo** (`ORDER BY node_id`). Ajustar el nº de conectores al throughput
  **real** de `asyncua` validado en el *spike* (plan §9.3/§14), no al teórico.
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
| Tags duplicados/huecos entre conectores | Particionamiento no coordinado | Asegurar partición por catálogo (`node_id`), no por browse ad-hoc |
| Latencia de escritura alta | Chunks grandes / BD subdimensionada | Revisar tuning (§15.2), chunk de 15 min, recursos del host de BD |

### Mantenimiento
- **Certificados OPC-UA:** validez recomendada ≤ 3 años. Alertar 30 días antes del vencimiento.
- **Retención/compresión:** compresión automática > 7 días; configurar `add_retention_policy`
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
