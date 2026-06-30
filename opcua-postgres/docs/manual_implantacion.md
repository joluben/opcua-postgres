# Manual de Implantación — Conector OPC-UA → TimescaleDB

**Versión:** 1.0  
**Fecha:** Junio 2026  
**Clasificación:** Documento técnico operativo

---

## Tabla de Contenidos

1. [Introducción y alcance](#1-introducción-y-alcance)
2. [Pre-requisitos](#2-pre-requisitos)
3. [Arquitectura de despliegue](#3-arquitectura-de-despliegue)
4. [Preparación del servidor de base de datos](#4-preparación-del-servidor-de-base-de-datos)
5. [Preparación del host del conector](#5-preparación-del-host-del-conector)
6. [Configuración de la imagen Docker](#6-configuración-de-la-imagen-docker)
7. [Caso de uso A — Un conector, sin autenticación OPC-UA](#7-caso-de-uso-a--un-conector-sin-autenticación-opc-ua)
8. [Caso de uso B — Un conector, con autenticación y cifrado OPC-UA](#8-caso-de-uso-b--un-conector-con-autenticación-y-cifrado-opc-ua)
9. [Caso de uso C — Múltiples conectores en paralelo (escalado horizontal)](#9-caso-de-uso-c--múltiples-conectores-en-paralelo-escalado-horizontal)
10. [Caso de uso D — PostgreSQL plano (sin TimescaleDB)](#10-caso-de-uso-d--postgresql-plano-sin-timescaledb)
11. [Caso de uso E — SSL verify-full con CA interna](#11-caso-de-uso-e--ssl-verify-full-con-ca-interna)
12. [Caso de uso F — Entorno de pruebas local autocontenido](#12-caso-de-uso-f--entorno-de-pruebas-local-autocontenido)
13. [Gestión de secretos y credenciales](#13-gestión-de-secretos-y-credenciales)
14. [Referencia completa de variables de entorno](#14-referencia-completa-de-variables-de-entorno)
15. [Operación y monitoreo](#15-operación-y-monitoreo)
16. [Procedimientos de mantenimiento](#16-procedimientos-de-mantenimiento)
17. [Resolución de problemas](#17-resolución-de-problemas)
18. [Runbook de incidentes](#18-runbook-de-incidentes)

---

## 1. Introducción y alcance

Este manual describe los pasos necesarios para instalar, configurar y operar el **conector OPC-UA → TimescaleDB** en entornos de producción. El conector:

- Se suscribe a cambios de valor (*DataChange*) en un servidor OPC-UA industrial.
- Persiste los datos en **TimescaleDB** (o PostgreSQL plano) con mínima latencia.
- Soporta **escalado horizontal** mediante múltiples instancias con particiones de tags.
- Dispone de **buffer de spill a disco** para tolerar caídas prolongadas de la base de datos.
- Expone métricas **Prometheus** en `/metrics` y un endpoint `/health`.

### Topología objetivo

```
┌────────────────────────┐        ┌─────────────────────────────────┐
│   Servidor OPC-UA      │        │  Host del conector (Linux)       │
│  (PLC / SCADA / DCS)   │◄──────►│  Docker Compose                  │
│  opc.tcp://....:4840   │        │  connector-01 … connector-N      │
└────────────────────────┘        └──────────────┬──────────────────┘
                                                  │ TCP 5432 (SSL)
                                  ┌───────────────▼─────────────────-─┐
                                  │  Servidor de BD (Linux separado)  │
                                  │  TimescaleDB 2.x sobre PG 16/17/18│
                                  └───────────────────────────────────┘
```

> **Importante:** la base de datos se despliega en un **servidor independiente**. El conector
> nunca levanta ni gestiona la base de datos.

---

## 2. Pre-requisitos

### 2.1 Software requerido en el host del conector

| Componente | Versión mínima | Notas |
|---|---|---|
| Linux (Ubuntu/Debian/RHEL) | Ubuntu 22.04 LTS | Kernel ≥ 5.15 recomendado |
| Docker Engine | 24.x | `docker.io` o `docker-ce` |
| Docker Compose plugin | v2.20 | `docker compose` (no `docker-compose` v1) |
| GNU Make | 4.x | Para usar el `Makefile` del proyecto |
| Git | 2.x | Para clonar el repositorio |

```bash
# Verificar versiones
docker --version          # Docker version 24.x
docker compose version    # Docker Compose version v2.x
make --version            # GNU Make 4.x
```

### 2.2 Software requerido en el servidor de base de datos

| Componente | Versión mínima |
|---|---|
| PostgreSQL | 16.x/17.x/18.x |
| TimescaleDB | 2.17.x |

> Si se usa **PostgreSQL plano** (sin TimescaleDB), ver [Caso de uso D](#10-caso-de-uso-d--postgresql-plano-sin-timescaledb).

### 2.3 Acceso de red necesario

| Origen | Destino | Puerto | Protocolo |
|---|---|---|---|
| Host conector | Servidor OPC-UA | 4840 (por defecto) | TCP |
| Host conector | Servidor BD | 5432 | TCP (SSL) |
| Prometheus / monitoring | Host conector | 8001–800N | TCP |

---

## 3. Arquitectura de despliegue

### 3.1 Componentes del repositorio

```
opcua-postgres/
├── connector/              ← Código fuente del conector (único artefacto en imagen)
│   ├── main.py             ← Punto de entrada
│   ├── config.py           ← Carga y validación de variables de entorno
│   ├── opc/                ← Cliente OPC-UA, browser de tags, suscripciones
│   └── db/                 ← Pool asyncpg, inicialización de schema, batch writer
├── Dockerfile              ← Build multi-stage; ARG VERSION; LABEL OCI
├── Makefile                ← Targets: build, push, up, down, test, logs
├── docker-compose.yml      ← Despliegue de 1 conector en producción
├── docker-compose.scale.yml← Despliegue de N conectores en paralelo
├── docker-compose.test.yml ← Pipeline de test local (BD + sim + conector)
├── scripts/
│   └── dba_setup.sql       ← Script de aprovisionamiento de BD (ejecutado por DBA)
├── secrets/                ← Credenciales (NUNCA versionadas en git)
│   ├── postgres_password.txt
│   └── opc_password.txt
├── certs/                  ← Certificados OPC-UA (NUNCA versionados en git)
├── .env                    ← Config local (derivado de .env.example; NO en git)
└── .env.example            ← Plantilla de configuración
```

### 3.2 Flujo de datos interno

```
OPC-UA Server
      │  DataChange notifications (subscripción asíncrona)
      ▼
 OpcSubscriptionHandler
      │  asyncio.Queue (buffer en memoria; MAX=OPC_QUEUE_MAX_SIZE)
      │  overflow → spill a disco (POSTGRES_SPILL_DIR)
      ▼
 BatchWriter
      │  acumula hasta POSTGRES_BATCH_SIZE filas o POSTGRES_FLUSH_INTERVAL_MS
      │  COPY binario a TimescaleDB (asyncpg)
      ▼
 TimescaleDB
      └── hypertable opc_raw_values (chunks 15 min, compresión > 7 días)
```

---

## 4. Preparación del servidor de base de datos

> Estos pasos los ejecuta el **DBA** en el servidor de BD, una sola vez.

### 4.1 Instalar TimescaleDB

Seguir la documentación oficial de TimescaleDB para PG 16/17/18:
```bash
# Ejemplo Ubuntu
sudo apt install postgresql-16-timescaledb
# Editar postgresql.conf:
#   shared_preload_libraries = 'timescaledb'
sudo systemctl restart postgresql
```

### 4.2 Crear base de datos, usuario y extensión

```bash
psql -U postgres -h localhost
```

```sql
-- Crear base de datos
CREATE DATABASE scada_db;
\c scada_db

-- Ejecutar el script de aprovisionamiento del repositorio
\i /ruta/a/opcua-postgres/scripts/dba_setup.sql
```

El script `dba_setup.sql` realiza:
1. `CREATE EXTENSION IF NOT EXISTS timescaledb;`
2. `CREATE USER connector_user WITH PASSWORD '...';`
3. `GRANT CONNECT, USAGE, SELECT, INSERT, UPDATE` con `ALTER DEFAULT PRIVILEGES`.

> **Sustituir `'SECRET'`** en el script por la contraseña real antes de ejecutarlo.

### 4.3 Tuning de `postgresql.conf` para alta ingesta

```ini
shared_buffers            = 8GB        # ~25% de RAM disponible
effective_cache_size      = 24GB
work_mem                  = 64MB
maintenance_work_mem      = 1GB
wal_level                 = replica    # Nunca 'minimal' en producción
max_wal_size              = 16GB
checkpoint_completion_target = 0.9
random_page_cost          = 1.1        # Si usa SSD NVMe
timescaledb.max_background_workers = 8
```

```bash
sudo systemctl reload postgresql
```

### 4.4 Habilitar SSL en el servidor de BD

```ini
# postgresql.conf
ssl      = on
ssl_cert_file = '/etc/ssl/certs/server.crt'
ssl_key_file  = '/etc/ssl/private/server.key'
# Para verify-full, también:
# ssl_ca_file = '/etc/ssl/certs/ca.crt'
```

```
# pg_hba.conf — forzar SSL desde los hosts del conector
hostssl  scada_db  connector_user  <IP_HOST_CONECTOR>/32  scram-sha-256
```

```bash
sudo systemctl reload postgresql
```

---

## 5. Preparación del host del conector

### 5.1 Clonar el repositorio

```bash
git clone https://github.com/joluben/opcua-postgres.git
cd opcua-postgres
```

### 5.2 Crear el fichero de configuración

```bash
cp .env.example .env
# Editar .env con los valores reales:
#   OPC_SERVER_URL, POSTGRES_HOST, POSTGRES_DB, POSTGRES_USER, etc.
nano .env
```

Variables **obligatorias** a rellenar:

| Variable | Ejemplo |
|---|---|
| `OPC_SERVER_URL` | `opc.tcp://plc.fabrica.local:4840` |
| `POSTGRES_HOST` | `db.fabrica.local` |
| `POSTGRES_DB` | `scada_db` |
| `POSTGRES_USER` | `connector_user` |

### 5.3 Crear los ficheros de secretos

```bash
# Contraseña del usuario connector_user en la BD
# Sin salto de línea final (echo -n es obligatorio)
echo -n "TU_PASSWORD_BD_REAL" > secrets/postgres_password.txt

# Contraseña OPC-UA (vacío si el servidor no requiere autenticación)
echo -n "" > secrets/opc_password.txt

# Verificar permisos (solo lectura para el propietario)
chmod 600 secrets/postgres_password.txt
chmod 600 secrets/opc_password.txt
```

> **Advertencia:** Un salto de línea final en el fichero causará un error de autenticación
> silencioso. Usar siempre `echo -n` o un editor que no añada newline.

---

## 6. Configuración de la imagen Docker

### 6.1 Construir la imagen localmente

```bash
# Tag 'dev' (por defecto)
make build

# Tag de versión semántica
make build VERSION=1.3.0
```

La imagen resultante tiene:
- Usuario no-root `connector` (UID no privilegiado).
- Directorio `/var/lib/connector/spill` con propietario `connector`.
- `LABEL` OCI con versión, descripción y licencia.
- `EXPOSE 8000` (métricas + health).

### 6.2 Publicar en registro privado

```bash
make push VERSION=1.3.0 REGISTRY=registry.fabrica.local
```

Esto ejecuta `docker build → docker tag → docker push` en secuencia.

### 6.3 Actualizar `docker-compose.yml` para usar la imagen del registro

```yaml
# docker-compose.yml — sustituir la línea image:
image: registry.fabrica.local/opcua-connector:1.3.0
```

> Eliminar la directiva `build: .` si se usa imagen pre-construida del registro.

---

## 7. Caso de uso A — Un conector, sin autenticación OPC-UA

**Escenario:** servidor OPC-UA en red local, modo de seguridad `None`, un único conector.

### 7.1 Configuración `.env`

```ini
OPC_SERVER_URL=opc.tcp://plc.fabrica.local:4840
OPC_SECURITY_MODE=None
OPC_SECURITY_POLICY=NoSecurity
OPC_PUBLISH_INTERVAL_MS=500

POSTGRES_HOST=db.fabrica.local
POSTGRES_PORT=5432
POSTGRES_DB=scada_db
POSTGRES_USER=connector_user
POSTGRES_SSL_MODE=require

POSTGRES_USE_TIMESCALE=true
POSTGRES_SPILL_ENABLED=true
POSTGRES_SPILL_MAX_MB=1024
LOG_LEVEL=INFO
LOG_FORMAT=json
METRICS_PORT=8000
```

### 7.2 Crear secret de BD

```bash
echo -n "password_bd" > secrets/postgres_password.txt
echo -n ""            > secrets/opc_password.txt
```

### 7.3 Levantar

```bash
make up
# o bien:
docker compose up -d
```

### 7.4 Verificar

```bash
make logs                          # tail de logs del conector
curl http://localhost:8001/health  # debe devolver {"status":"ok"}
curl http://localhost:8001/metrics # métricas Prometheus
```

---

## 8. Caso de uso B — Un conector, con autenticación y cifrado OPC-UA

**Escenario:** servidor OPC-UA requiere usuario/contraseña y `SecurityMode=SignAndEncrypt`.

### 8.1 Generar certificados del conector

```bash
mkdir -p certs
# Generar clave privada y certificado autofirmado del cliente OPC-UA
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout certs/client_key.pem \
  -out    certs/client_cert.pem \
  -days   730 \
  -subj "/CN=opcua-connector/O=Fabrica"
chmod 600 certs/client_key.pem
```

> El certificado `certs/client_cert.pem` debe **registrarse en el servidor OPC-UA** como
> cliente de confianza (procedimiento dependiente del proveedor del servidor).

### 8.2 Configuración `.env`

```ini
OPC_SERVER_URL=opc.tcp://plc.fabrica.local:4840
OPC_USERNAME=opcua_user
# La contraseña se inyecta vía secret (ver paso 8.3)
OPC_SECURITY_MODE=SignAndEncrypt
OPC_SECURITY_POLICY=Basic256Sha256
OPC_CERTIFICATE_PATH=/certs/client_cert.pem
OPC_PRIVATE_KEY_PATH=/certs/client_key.pem
OPC_PUBLISH_INTERVAL_MS=500
```

### 8.3 Crear secrets

```bash
echo -n "password_bd"       > secrets/postgres_password.txt
echo -n "password_opc_user" > secrets/opc_password.txt
chmod 600 secrets/*.txt
```

### 8.4 Levantar y verificar

```bash
make up
make logs
```

En los logs estructurados JSON debe aparecer:
```json
{"event": "opc_connected", "security_mode": "SignAndEncrypt", ...}
```

---

## 9. Caso de uso C — Múltiples conectores en paralelo (escalado horizontal)

**Escenario:** 20.000 tags, throughput superior al que un solo conector puede absorber;
se usan 4 conectores cada uno responsable de 5.000 tags.

### 9.1 Principio de particionamiento

El catálogo de tags se ordena por `node_id` de forma estable. Cada conector recibe
un rango `[OPC_TAG_OFFSET, OPC_TAG_OFFSET + OPC_TAG_LIMIT)`.

| Conector | `OPC_TAG_OFFSET` | `OPC_TAG_LIMIT` | Tags |
|---|---|---|---|
| connector-01 | 0 | 5000 | 0–4999 |
| connector-02 | 5000 | 5000 | 5000–9999 |
| connector-03 | 10000 | 5000 | 10000–14999 |
| connector-04 | 15000 | 5000 | 15000–19999 |

> **Nota:** el particionamiento asigna tags del catálogo, no se corresponde
> necesariamente con rangos de NodeID del servidor OPC-UA.

### 9.2 Despliegue con `docker-compose.scale.yml`

```bash
# Levanta los 4 conectores usando la configuración base de docker-compose.yml
docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --build
```

### 9.3 Verificar todos los conectores

```bash
# Estado de los contenedores
docker compose -f docker-compose.yml -f docker-compose.scale.yml ps

# Health de cada uno
for port in 8001 8002 8003 8004; do
  echo "--- connector en :$port ---"
  curl -s http://localhost:$port/health
done
```

### 9.4 Añadir un conector adicional

Editar `docker-compose.scale.yml` y añadir un nuevo servicio:

```yaml
  connector-05:
    extends:
      file: docker-compose.yml
      service: connector-01
    container_name: opc-connector-05
    environment:
      CONNECTOR_ID: connector-05
      OPC_TAG_OFFSET: "20000"
      OPC_TAG_LIMIT:  "5000"
    ports:
      - "8005:8000"
```

```bash
docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d connector-05
```

---

## 10. Caso de uso D — PostgreSQL plano (sin TimescaleDB)

**Escenario:** la base de datos es PostgreSQL estándar sin la extensión TimescaleDB.

### 10.1 Diferencias respecto al modo TimescaleDB

| Aspecto | TimescaleDB | PostgreSQL plano |
|---|---|---|
| Particionado de tiempo | Hypertable automática (chunks 15 min) | Tabla regular |
| Compresión | Automática (> 7 días) | No disponible |
| Rendimiento a largo plazo | Estable por compresión | Degrada con volumen |
| Requisito de extensión | `timescaledb` instalada | No requerida |

### 10.2 Configuración `.env`

```ini
POSTGRES_USE_TIMESCALE=false
```

El conector creará las tablas sin hypertable ni política de compresión.

### 10.3 Script de aprovisionamiento simplificado

En `dba_setup.sql`, omitir o comentar:

```sql
-- CREATE EXTENSION IF NOT EXISTS timescaledb;  ← comentar si no está disponible
```

---

## 11. Caso de uso E — SSL verify-full con CA interna

**Escenario:** el servidor de BD tiene certificado firmado por una CA interna corporativa;
se requiere verificación completa de identidad (CA + hostname).

### 11.1 Preparar el certificado CA

```bash
# Copiar el certificado de la CA interna al directorio de certs del conector
cp /ruta/a/ca_interna.crt certs/ca.pem
chmod 644 certs/ca.pem
```

### 11.2 Configurar el contexto SSL en asyncpg

El conector usa `ssl.create_default_context()` que carga automáticamente el store
de CA del sistema. Para una CA interna:

```bash
# Opción A: añadir la CA al store del sistema (dentro del contenedor no es persistente)
# Opción B: montar ca.pem y configurar PGSSLROOTCERT (pendiente soporte en config.py)
# Opción C (actual): añadir la CA al bundle del sistema en la imagen
```

Añadir al `Dockerfile` (stage runtime, antes de `USER connector`):

```dockerfile
COPY certs/ca.pem /usr/local/share/ca-certificates/fabrica-ca.crt
RUN update-ca-certificates
```

> Esta opción incrusta la CA en la imagen. Si la CA rota, hay que reconstruir la imagen.

### 11.3 Configuración `.env`

```ini
POSTGRES_SSL_MODE=verify-full
```

### 11.4 Verificar la cadena SSL

```bash
# Desde el host del conector, verificar que el certificado del servidor es válido
openssl s_client -connect db.fabrica.local:5432 -starttls postgres \
  -CAfile certs/ca.pem
# Debe terminar con: Verify return code: 0 (ok)
```

---

## 12. Caso de uso F — Entorno de pruebas local autocontenido

**Escenario:** validación local completa sin servidor OPC-UA ni BD externos.
Incluye TimescaleDB, simulador OPC-UA y conector.

### 12.1 Requisitos adicionales

- `docker-compose.test.yml` y `tools/` presentes en el repositorio.
- No se necesitan secretos ni certificados reales.

### 12.2 Levantar el pipeline de test

```bash
make test
# equivalente a:
# docker compose -f docker-compose.test.yml up --build
```

El pipeline levanta:
1. **TimescaleDB** — idéntico al de producción.
2. **opcua-sim** — servidor OPC-UA simulado (N tags, tasa configurable).
3. **connector** — instancia conectada al simulador.

### 12.3 Verificar ingesta

```bash
# Health del conector en el entorno de test
curl http://localhost:8001/health

# Contar filas ingresadas en TimescaleDB
docker compose -f docker-compose.test.yml exec timescaledb \
  psql -U connector_user -d scada_db \
  -c "SELECT COUNT(*), MIN(ts), MAX(ts) FROM opc_raw_values;"

# Ver métricas
curl http://localhost:8001/metrics | grep opc_
```

### 12.4 Probar resiliencia (spill)

```bash
# Pausar la BD para forzar spill
docker compose -f docker-compose.test.yml pause timescaledb
sleep 30  # el conector debe hacer spill a disco

# Verificar spill activo en logs
docker compose -f docker-compose.test.yml logs connector | grep spill

# Reanudar la BD
docker compose -f docker-compose.test.yml unpause timescaledb
# El conector debe reinyectar los datos del spill automáticamente
```

### 12.5 Limpiar

```bash
make test-down
# equivalente a:
# docker compose -f docker-compose.test.yml down -v
```

---

## 13. Gestión de secretos y credenciales

### 13.1 Mecanismo Docker Secrets

El conector implementa la convención `<VAR>_FILE`: si existe la variable `FOO_FILE`
apuntando a un fichero, su contenido tiene prioridad sobre la variable `FOO` en claro.

| Variable en claro | Variable `_FILE` | Fichero montado |
|---|---|---|
| `POSTGRES_PASSWORD` | `POSTGRES_PASSWORD_FILE` | `/run/secrets/postgres_password` |
| `OPC_PASSWORD` | `OPC_PASSWORD_FILE` | `/run/secrets/opc_password` |

### 13.2 Crear los ficheros de secrets

```bash
# Siempre sin salto de línea final (echo -n)
echo -n "MI_PASSWORD_BD"   > secrets/postgres_password.txt
echo -n "MI_PASSWORD_OPC"  > secrets/opc_password.txt
chmod 600 secrets/*.txt
```

### 13.3 Rotación de credenciales

1. Actualizar el fichero `secrets/<nombre>.txt` con la nueva contraseña.
2. Reiniciar el contenedor afectado:
   ```bash
   docker compose restart connector-01
   ```
3. Verificar en logs que la conexión se establece correctamente.

> Los secretos se montan como tmpfs en `/run/secrets/`; no quedan en disco del contenedor.

### 13.4 Integración con Vault / AWS Secrets Manager

Para entornos con secret manager externo, generar el fichero en tiempo de despliegue:

```bash
# Ejemplo con HashiCorp Vault
vault kv get -field=password secret/scada/connector > secrets/postgres_password.txt
chmod 600 secrets/postgres_password.txt
docker compose up -d
```

---

## 14. Referencia completa de variables de entorno

### 14.1 OPC-UA

| Variable | Obligatoria | Valor por defecto | Descripción |
|---|---|---|---|
| `OPC_SERVER_URL` | ✅ | — | URL del servidor: `opc.tcp://host:4840` |
| `OPC_USERNAME` | No | vacío | Usuario OPC-UA (si requiere auth) |
| `OPC_PASSWORD` | No | vacío | Contraseña OPC-UA en claro (usar `_FILE`) |
| `OPC_PASSWORD_FILE` | No | — | Ruta al fichero con la contraseña OPC-UA |
| `OPC_SECURITY_MODE` | No | `None` | `None` / `Sign` / `SignAndEncrypt` |
| `OPC_SECURITY_POLICY` | No | `Basic256Sha256` | Política de seguridad OPC-UA |
| `OPC_CERTIFICATE_PATH` | Cond. | — | Obligatorio si `SECURITY_MODE != None` |
| `OPC_PRIVATE_KEY_PATH` | Cond. | — | Obligatorio si `SECURITY_MODE != None` |
| `OPC_PUBLISH_INTERVAL_MS` | No | `500` | Intervalo de publicación en ms |
| `OPC_DATACHANGE_DEADBAND` | No | `0.0` | Deadband para filtrar cambios mínimos |
| `OPC_DEADBAND_TYPE` | No | `None` | `None` / `Absolute` / `Percent` |
| `OPC_NAMESPACE_INDEX` | No | — | Filtrar tags por namespace |
| `OPC_NODE_ID_FILTER` | No | — | Prefijo de NodeID para filtrar tags |
| `OPC_TAG_OFFSET` | No | `0` | Offset de partición (escalado horizontal) |
| `OPC_TAG_LIMIT` | No | `5000` | Número de tags por conector |
| `OPC_SESSION_TIMEOUT_MS` | No | `30000` | Timeout de sesión OPC-UA |
| `OPC_QUEUE_MAX_SIZE` | No | `500000` | Tamaño máximo de la cola en memoria |
| `OPC_LIB_LOG_LEVEL` | No | `WARNING` | Nivel de log de la librería `asyncua` |

### 14.2 Base de datos

| Variable | Obligatoria | Valor por defecto | Descripción |
|---|---|---|---|
| `POSTGRES_HOST` | ✅ | — | Hostname o IP del servidor de BD |
| `POSTGRES_PORT` | No | `5432` | Puerto PostgreSQL |
| `POSTGRES_DB` | ✅ | — | Nombre de la base de datos |
| `POSTGRES_USER` | ✅ | — | Usuario de aplicación |
| `POSTGRES_PASSWORD` | ✅ | — | Contraseña (usar `POSTGRES_PASSWORD_FILE`) |
| `POSTGRES_PASSWORD_FILE` | No | — | Ruta al fichero con la contraseña de BD |
| `POSTGRES_SSL_MODE` | No | `prefer` | `disable`/`require`/`verify-ca`/`verify-full` |
| `POSTGRES_CATALOG_TABLE` | No | `opc_tags_catalog` | Tabla de catálogo de tags |
| `POSTGRES_DATA_TABLE` | No | `opc_raw_values` | Tabla de series de tiempo |
| `POSTGRES_BATCH_SIZE` | No | `1000` | Filas por COPY batch |
| `POSTGRES_FLUSH_INTERVAL_MS` | No | `500` | Máximo tiempo entre flushes |
| `POSTGRES_POOL_MIN` | No | `2` | Conexiones mínimas del pool |
| `POSTGRES_POOL_MAX` | No | `10` | Conexiones máximas del pool |
| `POSTGRES_STATEMENT_CACHE_SIZE` | No | `100` | `0` si se usa pgBouncer transaction mode |
| `POSTGRES_USE_TIMESCALE` | No | `true` | `false` para PostgreSQL plano |

### 14.3 Spill a disco

| Variable | Obligatoria | Valor por defecto | Descripción |
|---|---|---|---|
| `POSTGRES_SPILL_ENABLED` | No | `true` | Activar buffer a disco ante caída de BD |
| `POSTGRES_SPILL_DIR` | No | `/var/lib/connector/spill` | Directorio de spill |
| `POSTGRES_SPILL_MAX_MB` | No | `1024` | Límite máximo de spill en MB |
| `POSTGRES_SPILL_SEGMENT_MB` | No | `64` | Tamaño de cada segmento de spill |

### 14.4 Operación

| Variable | Obligatoria | Valor por defecto | Descripción |
|---|---|---|---|
| `CONNECTOR_ID` | ✅ | — | Identificador único del conector |
| `LOG_LEVEL` | No | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `LOG_FORMAT` | No | `json` | `json` (producción) / `console` (desarrollo) |
| `METRICS_PORT` | No | `8000` | Puerto del servidor HTTP de métricas |
| `RECONNECT_MAX_RETRIES` | No | `10` | Reintentos de reconexión OPC-UA |
| `RECONNECT_BASE_DELAY_S` | No | `2.0` | Delay base para backoff exponencial |

---

## 15. Operación y monitoreo

### 15.1 Comandos habituales

```bash
# Estado de los contenedores
make ps

# Logs en tiempo real
make logs                          # conector-01 por defecto
make logs SERVICE=connector-02     # conector específico

# Reiniciar un conector
docker compose restart connector-01

# Parar sin eliminar datos
make down

# Reconstruir e iniciar (tras cambio de configuración)
docker compose up -d --build connector-01
```

### 15.2 Endpoints de observabilidad

| Endpoint | Descripción |
|---|---|
| `GET /health` | `{"status":"ok"}` si el conector está operativo |
| `GET /metrics` | Métricas Prometheus en formato text/plain |

Métricas clave expuestas:

| Métrica | Descripción |
|---|---|
| `opc_values_received_total` | Valores recibidos del servidor OPC-UA |
| `opc_values_written_total` | Valores escritos en BD |
| `opc_queue_size` | Tamaño actual de la cola en memoria |
| `opc_spill_bytes` | Bytes acumulados en spill a disco |
| `opc_reconnections_total` | Reconexiones al servidor OPC-UA |
| `db_copy_duration_seconds` | Latencia del COPY a TimescaleDB |

### 15.3 Configurar scrape Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: opcua_connectors
    static_configs:
      - targets:
          - "host_conector:8001"   # connector-01
          - "host_conector:8002"   # connector-02
          # añadir un target por conector
    scrape_interval: 15s
```

### 15.4 Dashboard Grafana recomendado

Paneles sugeridos:

1. **Throughput**: `rate(opc_values_written_total[1m])` vs `rate(opc_values_received_total[1m])`
2. **Cola**: `opc_queue_size` (alerta si > 80% de `OPC_QUEUE_MAX_SIZE`)
3. **Spill**: `opc_spill_bytes` (alerta si crece sostenidamente)
4. **Latencia COPY**: histograma de `db_copy_duration_seconds`
5. **Reconexiones**: `rate(opc_reconnections_total[5m])`

---

## 16. Procedimientos de mantenimiento

### 16.1 Actualizar la imagen del conector

```bash
# 1. Construir y publicar nueva versión
make build VERSION=1.4.0
make push  VERSION=1.4.0 REGISTRY=registry.fabrica.local

# 2. Actualizar docker-compose.yml con el nuevo tag
#    image: registry.fabrica.local/opcua-connector:1.4.0

# 3. Redesplegar con cero downtime (el spill cubre la ventana de reinicio)
docker compose up -d --no-deps connector-01
```

### 16.2 Rotar credenciales de BD

```bash
# 1. Cambiar la contraseña en PostgreSQL (con DBA)
psql -U postgres -c "ALTER USER connector_user PASSWORD 'NUEVA_PASSWORD';"

# 2. Actualizar el secret
echo -n "NUEVA_PASSWORD" > secrets/postgres_password.txt

# 3. Reiniciar el conector (lee el secret en el arranque)
docker compose restart connector-01
```

### 16.3 Rotar certificados OPC-UA

```bash
# 1. Generar nuevo par de claves
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout certs/client_key.pem \
  -out    certs/client_cert.pem \
  -days   730 -subj "/CN=opcua-connector/O=Fabrica"

# 2. Registrar client_cert.pem en el servidor OPC-UA como cliente de confianza

# 3. Reiniciar el conector (los certs se montan como volumen :ro)
docker compose restart connector-01
```

### 16.4 Ampliar el límite de spill

```bash
# Editar .env o el environment del compose:
POSTGRES_SPILL_MAX_MB=4096

# Recargar configuración
docker compose up -d connector-01
```

### 16.5 Operaciones de mantenimiento en TimescaleDB

```sql
-- Ver estado de compresión
SELECT hypertable_name,
       pg_size_pretty(before_compression_total_bytes) AS antes,
       pg_size_pretty(after_compression_total_bytes)  AS despues,
       ROUND(100 - after_compression_total_bytes * 100.0 /
             NULLIF(before_compression_total_bytes, 0), 1) AS pct_reduccion
FROM timescaledb_information.hypertable_compression_stats;

-- Ver chunks activos
SELECT chunk_name, range_start, range_end,
       pg_size_pretty(total_bytes) AS total
FROM timescaledb_information.chunks
WHERE hypertable_name = 'opc_raw_values'
ORDER BY range_start DESC
LIMIT 20;

-- Añadir política de retención (p.ej. borrar datos > 90 días)
SELECT add_retention_policy('opc_raw_values', INTERVAL '90 days');
```

---

## 17. Resolución de problemas

### 17.1 El conector no arranca

**Síntoma:** `docker compose ps` muestra `Exit 1` inmediatamente.

```bash
docker compose logs connector-01 | tail -50
```

Causas habituales y solución:

| Error en log | Causa | Solución |
|---|---|---|
| `Variable de entorno obligatoria ausente: OPC_SERVER_URL` | Falta variable en `.env` | Añadir al `.env` |
| `Variable de entorno obligatoria ausente: POSTGRES_PASSWORD` | Secret no creado | Crear `secrets/postgres_password.txt` |
| `OPC_CERTIFICATE_PATH y OPC_PRIVATE_KEY_PATH son obligatorios` | `SECURITY_MODE != None` sin certs | Generar certs o poner `OPC_SECURITY_MODE=None` |
| `ConfigError: POSTGRES_BATCH_SIZE debe ser entero` | Valor no numérico en `.env` | Corregir el valor |

### 17.2 Error de conexión a la base de datos

```bash
# Verificar conectividad de red desde el contenedor
docker compose exec connector-01 \
  python -c "import socket; socket.create_connection(('db.fabrica.local', 5432), 5)"
```

| Error | Causa | Solución |
|---|---|---|
| `Connection refused` | BD no accesible | Verificar POSTGRES_HOST, firewall, que PostgreSQL esté activo |
| `SSL connection required` | BD exige SSL y no está configurado | Poner `POSTGRES_SSL_MODE=require` |
| `SSL SYSCALL error: EOF` | Mismatch de versión SSL / cert caducado | Verificar certs de BD; actualizar si necesario |
| `password authentication failed` | Contraseña incorrecta | Verificar `secrets/postgres_password.txt` (sin newline) |
| `extension "timescaledb" does not exist` | TimescaleDB no instalada en BD | Ejecutar `dba_setup.sql` o poner `POSTGRES_USE_TIMESCALE=false` |
| `permission denied for table` | Permisos insuficientes | Re-ejecutar los `GRANT` de `dba_setup.sql` |

### 17.3 El conector se conecta pero no ingesta datos

```bash
# Ver métricas en tiempo real
watch -n2 'curl -s http://localhost:8001/metrics | grep opc_values'
```

Posibles causas:

- **`opc_values_received_total` no crece**: el servidor OPC-UA no envía cambios. Verificar
  que los NodeIDs suscritos existen y tienen el namespace correcto (`OPC_NAMESPACE_INDEX`).
- **`opc_queue_size` crece sin límite**: el writer no puede conectar con BD. Revisar logs
  de BD (`db_copy_error`).
- **`opc_spill_bytes` crece**: BD desconectada o lenta; los datos se están acumulando en
  disco. Normal mientras dure la caída; se reinyectarán al recuperarse.

### 17.4 Alto uso de CPU o memoria

```bash
docker stats connector-01
```

| Síntoma | Causa probable | Solución |
|---|---|---|
| CPU > 80% sostenido | `OPC_PUBLISH_INTERVAL_MS` muy bajo | Aumentar a 200–500 ms |
| RAM > 400 MB | Cola en memoria muy grande | Reducir `OPC_QUEUE_MAX_SIZE` o aumentar límite |
| Spill creciente + BD activa | `POSTGRES_BATCH_SIZE` demasiado pequeño | Aumentar a 2000–5000 |

### 17.5 `opc_password.txt` vacío produce error

Si el servidor OPC-UA no requiere autenticación pero `OPC_SECURITY_MODE=None`,
el campo contraseña puede quedar vacío sin error. Si produce un error inesperado:

```bash
# Verificar que el fichero no tiene caracteres ocultos
xxd secrets/opc_password.txt
# Debe mostrar vacío o solo el contenido esperado
```

---

## 18. Runbook de incidentes

### 18.1 Caída de la base de datos

**Detección:** alerta Prometheus `opc_spill_bytes > 100MB` o `db_connected = 0`.

```
Tiempo 0  → BD cae
          → BatchWriter detecta error de conexión
          → Cola en memoria se llena → spill a disco en POSTGRES_SPILL_DIR
          → Logs: {"event": "db_disconnected", "spill_active": true}

Tiempo T  → BD se recupera
          → Pool asyncpg reconecta automáticamente (backoff exponencial)
          → BatchWriter reinyecta segmentos de spill en orden
          → Logs: {"event": "db_reconnected", "spill_bytes_reinjected": N}
          → opc_spill_bytes vuelve a 0
```

**Acción operativa:**
1. Verificar que la BD está activa: `psql -U connector_user -h db.fabrica.local -c '\l'`
2. Confirmar reinyección: `curl http://localhost:8001/metrics | grep spill`
3. Verificar integridad: `SELECT COUNT(*) FROM opc_raw_values WHERE ts > now() - interval '1h';`

**Si el spill llega al límite (`POSTGRES_SPILL_MAX_MB`):**
- Los datos más antiguos del spill se descartan (comportamiento FIFO).
- Ampliar el límite: `POSTGRES_SPILL_MAX_MB=4096` y reiniciar el conector.

### 18.2 Saturación de la cola en memoria

**Detección:** `opc_queue_size / OPC_QUEUE_MAX_SIZE > 0.8` durante > 60 s.

**Causas:** tasa de ingesta > capacidad de escritura en BD, o BD lenta.

**Acción:**
1. Aumentar `POSTGRES_BATCH_SIZE` (más filas por COPY).
2. Reducir `POSTGRES_FLUSH_INTERVAL_MS` (flushes más frecuentes).
3. Si la BD es el cuello de botella: revisar `shared_buffers`, índices y locks.
4. Si el servidor OPC-UA envía demasiado volumen: aumentar `OPC_DATACHANGE_DEADBAND`.

### 18.3 Reconexiones frecuentes al servidor OPC-UA

**Detección:** `rate(opc_reconnections_total[5m]) > 1`.

**Acción:**
1. Verificar conectividad de red: `ping plc.fabrica.local` desde el host del conector.
2. Aumentar `OPC_SESSION_TIMEOUT_MS` (default 30000 ms) si la red tiene latencia alta.
3. Verificar que los certificados OPC-UA no han caducado:
   ```bash
   openssl x509 -in certs/client_cert.pem -noout -dates
   ```
4. Revisar logs del servidor OPC-UA (PLC/SCADA) para errores de sesión.

### 18.4 Certificado OPC-UA próximo a vencer

**Detección:** `openssl x509 -in certs/client_cert.pem -noout -dates` muestra
`notAfter` en < 30 días.

**Acción:**
```bash
# 1. Generar nuevo certificado
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout certs/client_key.pem \
  -out    certs/client_cert.pem \
  -days   730 -subj "/CN=opcua-connector/O=Fabrica"

# 2. Registrar el nuevo certificado en el servidor OPC-UA

# 3. Reiniciar conectores
docker compose restart
```

### 18.5 Disco del host lleno por spill

**Detección:** `df -h` muestra > 90% en el filesystem del volumen `connector_spill`.

**Acción inmediata:**
```bash
# 1. Ver cuánto ocupa el spill
docker compose exec connector-01 du -sh /var/lib/connector/spill/

# 2. Si la BD está activa, el spill debería drenarse solo.
#    Esperar a que opc_spill_bytes vuelva a 0.

# 3. Si la BD no está disponible, decidir si se puede tolerar pérdida de datos:
#    Limpiar manualmente solo si se acepta la pérdida:
docker compose exec connector-01 rm -rf /var/lib/connector/spill/<CONNECTOR_ID>/*
```

---

*Fin del documento — Manual de Implantación Conector OPC-UA v1.0*
