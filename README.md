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
> recuperarse. El directorio de spill debe estar en un **volumen persistente**.

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
│   ├── config.py            # Carga/validación de variables de entorno (+ Docker Secrets *_FILE)
│   ├── opc/
│   │   ├── client.py        # Sesión OPC-UA
│   │   ├── security.py      # Políticas y certificados X.509
│   │   ├── browser.py       # Descubrimiento + partición vía catálogo
│   │   └── subscription.py  # DataChange → asyncio.Queue
│   ├── db/
│   │   ├── pool.py          # Pool asyncpg (SSL, statement_cache para pgBouncer)
│   │   ├── initializer.py   # Creación idempotente de tablas/hypertable + verificación de permisos
│   │   └── writer.py        # Batch writer con COPY + drop-oldest
│   └── utils/
│       ├── logger.py        # structlog (JSON)
│       ├── metrics.py       # Prometheus + /health (aiohttp)
│       └── resilience.py    # Backoff exponencial con jitter
├── scripts/dba_setup.sql    # Aprovisionamiento del servidor de BD (ejecuta el DBA)
├── tests/                   # test_security.py, test_browser.py, test_writer.py
├── Dockerfile               # Multi-stage, usuario no-root
├── docker-compose.yml       # Un conector (BD remota)
├── docker-compose.scale.yml # Varios conectores en paralelo
├── .env.example
└── requirements.txt
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

## Tests

```bash
pip install -r requirements.txt pytest
pytest -q
```

Los tests incluidos no requieren servidor OPC-UA ni BD (validan configuración, filtros de
browse y la política drop-oldest del writer).

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
| `opc_connector_spill_dropped_total` o `values_dropped_total` crecen | Spill lleno (`POSTGRES_SPILL_MAX_MB`) o spill deshabilitado | Ampliar `POSTGRES_SPILL_MAX_MB`/disco, o `OPC_QUEUE_MAX_SIZE`; definir SLA de pérdida |
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
- BD con `POSTGRES_SSL_MODE=require` en producción.
- Contenedor sin root; usuario de BD con permisos mínimos.
- Nunca loggear variables de entorno completas ni valores de proceso.
