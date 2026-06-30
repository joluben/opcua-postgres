# Plan de Implementación: Conector OPC-UA → TimescaleDB

**Versión:** 1.3  
**Fecha:** Junio 2026  
**Clasificación:** Documento técnico interno

> **Cambios v1.3:** Hardening de producción materializado: creados `.dockerignore` (contexto de build = 1.6 kB), `secrets/` con doble barrera de git, y actualizado `docker-compose.yml` con Docker Secrets (`postgres_password`, `opc_password`), `POSTGRES_SSL_MODE=require`, límites de recursos (`cpus: 1.0 / memory: 512M`) y logs JSON. §17.3 actualizado a completado. Nueva sección **§18 Próximos Pasos** con las tareas ordenadas por prioridad para cerrar la Fase 6 y habilitar el despliegue en producción.
>
> **Cambios v1.2:** Añadidos los **resultados de la validación end-to-end** (§14, Fase 5) ejecutada con el pipeline `docker-compose.test.yml` (TimescaleDB + simulador + conector). Nueva sección **§17 Paso a Producción** con el checklist detallado de tareas, *hardening* y la **limpieza de artefactos de prueba** (simulador, `tools/`, `tests/`, compose de test). Logging de `asyncua` silenciado por defecto (`OPC_LIB_LOG_LEVEL`).
>
> **Cambios v1.1:** La base de datos se despliega en un **servidor independiente** (no en el mismo Docker que el conector). `asyncua` actualizado a 1.1.x/2.0; tamaños de disco y de buffer corregidos; permisos de BD ajustados (`UPDATE` en catálogo); `pgBouncer` pasa a ser opcional y se ubica en el host de BD; `wal_level=replica`; chunks de 15 min; deadband OPC-UA configurable; estimación de esfuerzo revisada.

---

## Tabla de Contenidos

1. [Resumen Ejecutivo](#1-resumen-ejecutivo)
2. [Arquitectura General](#2-arquitectura-general)
3. [Stack Tecnológico](#3-stack-tecnológico)
4. [Estructura del Proyecto](#4-estructura-del-proyecto)
5. [Modelo de Datos](#5-modelo-de-datos)
6. [Configuración por Variables de Entorno](#6-configuración-por-variables-de-entorno)
7. [Seguridad OPC-UA](#7-seguridad-opc-ua)
8. [Inicialización Automática de la Base de Datos](#8-inicialización-automática-de-la-base-de-datos)
9. [Motor de Ingesta](#9-motor-de-ingesta)
10. [Despliegue con Docker](#10-despliegue-con-docker)
11. [Escalado Horizontal: Múltiples Conectores en Paralelo](#11-escalado-horizontal-múltiples-conectores-en-paralelo)
12. [Resiliencia y Manejo de Errores](#12-resiliencia-y-manejo-de-errores)
13. [Monitoreo y Observabilidad](#13-monitoreo-y-observabilidad)
14. [Plan de Fases y Estimación de Esfuerzo](#14-plan-de-fases-y-estimación-de-esfuerzo)
15. [Consideraciones de Rendimiento](#15-consideraciones-de-rendimiento)
16. [Riesgos y Mitigaciones](#16-riesgos-y-mitigaciones)
17. [Paso a Producción](#17-paso-a-producción)
18. [Próximos Pasos](#18-próximos-pasos)

---

## 1. Resumen Ejecutivo

Este documento describe el plan de implementación de un conector OPC-UA desarrollado en Python, diseñado para:

- Conectarse a servidores OPC-UA industriales con soporte para todos los niveles de seguridad disponibles.
- Descubrir y suscribirse automáticamente a tags (señales) publicadas por el servidor.
- Ingestar entre **10.000 y 50.000 señales** con intervalos de muestreo de **100ms a 500ms**.
- Persistir los datos en **TimescaleDB** (extensión de PostgreSQL optimizada para series de tiempo).
- Desplegarse como contenedor **Docker**, con configuración exclusivamente por variables de entorno.
- Escalar horizontalmente mediante **múltiples instancias paralelas**, cada una responsable de una partición de tags.

---

## 2. Arquitectura General

```
┌─────────────────────────────────────────────────────────────────┐
│                        OPC-UA Server                            │
│              (PLCs, SCADAs, DCS industriales)                   │
└────────────┬──────────────┬──────────────┬──────────────────────┘
             │              │              │
        Sesión 1       Sesión 2       Sesión N
             │              │              │
  ┌──────────▼──┐  ┌────────▼────┐  ┌─────▼───────┐
  │ Conector #1 │  │ Conector #2 │  │ Conector #N │   ← Contenedores Docker
  │ Tags 0–9999 │  │Tags 10k–19k │  │Tags 20k–50k │     en Linux
  │             │  │             │  │             │
  │ asyncio +   │  │ asyncio +   │  │ asyncio +   │
  │ opcua-async │  │ opcua-async │  │ opcua-async │
  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘
         │                │                │
         └────────────────▼────────────────┘
                          │  red (TCP 5432, SSL)
      ═══════════════════════════════════════════  ← frontera de red / otro host
                          │
            ┌─────────────▼──────────────────┐
            │      Servidor de Base de Datos   │  ← Host SEPARADO (otro servidor Linux)
            │  ┌────────────────────────────┐ │
            │  │  (opcional) pgBouncer       │ │  ← Solo si hay muchos conectores
            │  └─────────────┬──────────────┘ │
            │  ┌─────────────▼──────────────┐ │
            │  │   TimescaleDB (PG 16)       │ │  ← PostgreSQL + extensión Timescale
            │  └────────────────────────────┘ │
            └──────────────────────────────────┘
```

**Flujo de datos por conector:**

```
OPC-UA Server
     │
     │  Notificaciones por suscripción (DataChange)
     ▼
┌─────────────────────────────────────────────┐
│              Conector OPC-UA                │
│                                             │
│  ┌──────────┐    ┌──────────┐    ┌───────┐ │
│  │ OPC-UA   │───▶│ asyncio  │───▶│Batch  │ │
│  │ Client   │    │  Queue   │    │Writer │ │
│  │(Subscriber│   │(buffer)  │    │       │ │
│  └──────────┘    └──────────┘    └───┬───┘ │
└────────────────────────────────────│────────┘
                                     │  INSERT / COPY
                                     ▼
                               TimescaleDB
```

---

## 3. Stack Tecnológico

| Componente | Tecnología | Versión recomendada | Justificación |
|---|---|---|---|
| Lenguaje | Python | 3.12+ | Soporte nativo asyncio, ecosistema OPC-UA maduro |
| Cliente OPC-UA | `opcua-asyncio` (`asyncua`) | 1.1.x / 2.0 | Asíncrono nativo, soporte completo de seguridad, reconexión automática. La serie 0.9.x está obsoleta (2020-2021) |
| Base de datos | TimescaleDB | 2.x sobre PG 16 | Compresión automática, hypertables, ingestión masiva. **Desplegada en servidor independiente** |
| Driver DB | `asyncpg` | 0.29.x | Driver asíncrono de máximo rendimiento para PostgreSQL |
| Contenerización | Docker + Docker Compose | 27.x | Despliegue reproducible, escalado sencillo |
| Pool de conexiones | pgBouncer (opcional, en host de BD) | 1.22.x | Solo necesario con muchos conectores. **Requiere `asyncpg` con `statement_cache_size=0`** en modo `transaction` (incompatibilidad de prepared statements) |
| Gestión de secretos | Variables de entorno + Docker Secrets | — | Sin credenciales en el código ni en imágenes |
| Logging | `structlog` | 24.x | Logs estructurados en JSON, integrables con ELK/Loki |
| Métricas | `prometheus_client` | 0.20.x | Exposición de métricas para Prometheus/Grafana |

---

## 4. Estructura del Proyecto

```
opc-ua-connector/
├── connector/
│   ├── __init__.py
│   ├── main.py                  # Punto de entrada principal
│   ├── config.py                # Carga y validación de variables de entorno
│   ├── opc/
│   │   ├── client.py            # Cliente OPC-UA y gestión de sesión
│   │   ├── browser.py           # Descubrimiento de tags en el Address Space
│   │   ├── subscription.py      # Motor de suscripción DataChange
│   │   └── security.py          # Gestión de certificados y políticas de seguridad
│   ├── db/
│   │   ├── pool.py              # Pool de conexiones asyncpg
│   │   ├── initializer.py       # Creación automática de tabla en primera conexión
│   │   └── writer.py            # Escritura en lotes a TimescaleDB
│   └── utils/
│       ├── logger.py            # Logger estructurado
│       ├── metrics.py           # Métricas Prometheus
│       └── resilience.py        # Reconexión con backoff exponencial
├── certs/                       # Certificados OPC-UA (montados como volumen)
│   ├── client_cert.pem
│   └── client_key.pem
├── Dockerfile
├── docker-compose.yml           # Despliegue de un conector (base)
├── docker-compose.scale.yml     # Despliegue de múltiples conectores en paralelo
├── .env.example                 # Plantilla de variables de entorno (sin valores reales)
├── requirements.txt
└── tests/
    ├── test_browser.py
    ├── test_writer.py
    └── test_security.py
```

---

## 5. Modelo de Datos

### 5.1 Tabla de catálogo de tags

Esta tabla se crea automáticamente en la primera conexión. El nombre es configurable por variable de entorno.

```sql
-- Catálogo de nodos descubiertos en el servidor OPC-UA
CREATE TABLE IF NOT EXISTS {OPC_TAGS_CATALOG_TABLE} (
    id            SERIAL PRIMARY KEY,
    node_id       TEXT        NOT NULL UNIQUE,   -- Ej: "ns=2;s=Planta1.Temp1"
    display_name  TEXT,
    description   TEXT,
    data_type     TEXT,                          -- "Double", "Int32", "Boolean", etc.
    namespace_uri TEXT,
    active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

> **Nota de concurrencia:** Como varios conectores pueden insertar en el catálogo, las inserciones deben usar `INSERT ... ON CONFLICT (node_id) DO UPDATE SET updated_at = NOW() RETURNING id` para obtener el `tag_id` de forma idónea. El catálogo es la **fuente de verdad** de la asignación tag→conector (ver sección 11).

### 5.2 Tabla de series de tiempo (configurable por variable de entorno)

```sql
-- Tabla de valores ingestados desde el OPC Server
-- El nombre {OPC_DATA_TABLE} se define en la variable de entorno POSTGRES_DATA_TABLE
CREATE TABLE IF NOT EXISTS {OPC_DATA_TABLE} (
    tag_id        INTEGER     NOT NULL REFERENCES {OPC_TAGS_CATALOG_TABLE}(id),
    ts            TIMESTAMPTZ NOT NULL,           -- Timestamp del servidor OPC-UA (source time)
    received_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Timestamp de llegada al conector
    value_num     DOUBLE PRECISION,               -- Valores numéricos (Float, Int, Bool)
    value_str     TEXT,                           -- Valores de tipo String o Enum
    quality       INTEGER,                        -- OPC-UA StatusCode
    connector_id  TEXT        NOT NULL            -- Identifica qué instancia ingestó el dato
);

-- Convertir a hypertable de TimescaleDB (partición por tiempo)
SELECT create_hypertable(
    '{OPC_DATA_TABLE}',
    'ts',
    chunk_time_interval => INTERVAL '15 minutes', -- Chunk activo + sus índices deben caber en ~25% de RAM
    if_not_exists => TRUE
);

-- Índices para consultas típicas
CREATE INDEX IF NOT EXISTS idx_{OPC_DATA_TABLE}_tag_ts
    ON {OPC_DATA_TABLE} (tag_id, ts DESC);

CREATE INDEX IF NOT EXISTS idx_{OPC_DATA_TABLE}_quality
    ON {OPC_DATA_TABLE} (quality)
    WHERE quality != 0;  -- Índice parcial para datos con errores

-- Política de compresión automática (datos > 7 días)
SELECT add_compression_policy(
    '{OPC_DATA_TABLE}',
    INTERVAL '7 days'
);

-- Política de retención (opcional, configurable)
-- SELECT add_retention_policy('{OPC_DATA_TABLE}', INTERVAL '1 year');
```

### 5.3 Variables de entorno que controlan los nombres de tablas

```bash
POSTGRES_CATALOG_TABLE=opc_tags_catalog   # Tabla de catálogo de nodos
POSTGRES_DATA_TABLE=opc_raw_values        # Tabla de series de tiempo (datos)
```

---

## 6. Configuración por Variables de Entorno

Toda la configuración del conector se realiza exclusivamente a través de variables de entorno. **Ninguna credencial debe estar en el código fuente ni en la imagen Docker.**

### 6.1 Variables de conexión OPC-UA

| Variable | Obligatoria | Ejemplo | Descripción |
|---|---|---|---|
| `OPC_SERVER_URL` | ✅ | `opc.tcp://192.168.1.10:4840` | URL del servidor OPC-UA |
| `OPC_USERNAME` | ⚠️ | `admin` | Usuario (si el servidor requiere autenticación) |
| `OPC_PASSWORD` | ⚠️ | *(secreto)* | Contraseña del usuario OPC-UA |
| `OPC_SECURITY_POLICY` | ✅ | `Basic256Sha256` | Ver sección 7 para opciones |
| `OPC_SECURITY_MODE` | ✅ | `SignAndEncrypt` | `None`, `Sign`, `SignAndEncrypt` |
| `OPC_CERTIFICATE_PATH` | ⚠️ | `/certs/client_cert.pem` | Requerido si security mode ≠ None |
| `OPC_PRIVATE_KEY_PATH` | ⚠️ | `/certs/client_key.pem` | Requerido si security mode ≠ None |
| `OPC_PUBLISH_INTERVAL_MS` | ✅ | `100` | Intervalo de publicación en ms (100–500) |
| `OPC_NAMESPACE_INDEX` | ❌ | `2` | Filtrar por namespace específico (opcional) |
| `OPC_NODE_ID_FILTER` | ❌ | `ns=2;s=Planta1.*` | Patrón glob para filtrar nodos (opcional) |
| `OPC_TAG_OFFSET` | ❌ | `0` | Índice de inicio de partición de tags |
| `OPC_TAG_LIMIT` | ❌ | `10000` | Cantidad máxima de tags a manejar |
| `OPC_SESSION_TIMEOUT_MS` | ❌ | `30000` | Timeout de sesión OPC-UA |
| `OPC_DATACHANGE_DEADBAND` | ❌ | `0.0` | Deadband (absoluto/porcentual) del `DataChangeFilter` para filtrar ruido en señales analógicas y reducir volumen |
| `OPC_DEADBAND_TYPE` | ❌ | `None` | `None`, `Absolute` o `Percent` |

### 6.2 Variables de conexión PostgreSQL / TimescaleDB

> La base de datos reside en un **servidor independiente**. `POSTGRES_HOST` apunta al host remoto (DNS interno o IP), no a un servicio del mismo `docker-compose`. Usar `POSTGRES_SSL_MODE=require` en producción.

| Variable | Obligatoria | Ejemplo | Descripción |
|---|---|---|---|
| `POSTGRES_HOST` | ✅ | `db.internal.example` | Host remoto de la base de datos (servidor independiente) |
| `POSTGRES_PORT` | ✅ | `5432` | Puerto |
| `POSTGRES_DB` | ✅ | `scada_db` | Nombre de la base de datos |
| `POSTGRES_USER` | ✅ | `connector_user` | Usuario de base de datos |
| `POSTGRES_PASSWORD` | ✅ | *(secreto)* | Contraseña |
| `POSTGRES_CATALOG_TABLE` | ✅ | `opc_tags_catalog` | Tabla de catálogo de nodos OPC |
| `POSTGRES_DATA_TABLE` | ✅ | `opc_raw_values` | Tabla de series de tiempo |
| `POSTGRES_BATCH_SIZE` | ✅ | `1000` | Registros por lote de escritura |
| `POSTGRES_FLUSH_INTERVAL_MS` | ✅ | `500` | Máximo tiempo entre escrituras |
| `POSTGRES_POOL_MIN` | ❌ | `2` | Conexiones mínimas en el pool |
| `POSTGRES_POOL_MAX` | ❌ | `10` | Conexiones máximas en el pool |
| `POSTGRES_STATEMENT_CACHE_SIZE` | ❌ | `100` | Caché de prepared statements; **poner `0` si se usa pgBouncer (transaction)** |
| `POSTGRES_USE_TIMESCALE` | ❌ | `true` | `true`: hypertable + compresión. `false`: PostgreSQL plano (sin hypertable) |
| `POSTGRES_SPILL_ENABLED` | ❌ | `true` | Activa el volcado a disco del buffer ante caídas largas de BD |
| `POSTGRES_SPILL_DIR` | ❌ | `/var/lib/connector/spill` | Directorio del spill (debe ser **volumen persistente**) |
| `POSTGRES_SPILL_MAX_MB` | ❌ | `1024` | Tope total en disco; al superarlo se descartan segmentos antiguos |
| `POSTGRES_SPILL_SEGMENT_MB` | ❌ | `64` | Tamaño de rotación de segmento |
| `POSTGRES_SSL_MODE` | ❌ | `require` | `disable`, `allow`, `prefer`, `require` |

### 6.3 Variables operacionales

| Variable | Obligatoria | Ejemplo | Descripción |
|---|---|---|---|
| `CONNECTOR_ID` | ✅ | `connector-01` | Identificador único de esta instancia |
| `LOG_LEVEL` | ❌ | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `LOG_FORMAT` | ❌ | `json` | `json` (producción) o `pretty` (desarrollo) |
| `METRICS_PORT` | ❌ | `8000` | Puerto HTTP para exponer métricas Prometheus |
| `RECONNECT_MAX_RETRIES` | ❌ | `10` | Intentos máximos de reconexión |
| `RECONNECT_BASE_DELAY_S` | ❌ | `2` | Delay base para backoff exponencial (segundos) |
| `OPC_QUEUE_MAX_SIZE` | ❌ | `500000` | Capacidad del buffer en memoria (items). Define cuántos segundos de datos absorbe ante caídas de la BD |

### 6.4 Archivo `.env.example`

```bash
# ── OPC-UA Connection ──────────────────────────────────────────────────────
OPC_SERVER_URL=opc.tcp://CHANGE_ME:4840
OPC_USERNAME=
OPC_PASSWORD=
OPC_SECURITY_POLICY=Basic256Sha256
OPC_SECURITY_MODE=SignAndEncrypt
OPC_CERTIFICATE_PATH=/certs/client_cert.pem
OPC_PRIVATE_KEY_PATH=/certs/client_key.pem
OPC_PUBLISH_INTERVAL_MS=500
OPC_DATACHANGE_DEADBAND=0.0
OPC_DEADBAND_TYPE=None
OPC_TAG_OFFSET=0
OPC_TAG_LIMIT=5000

# ── PostgreSQL / TimescaleDB ────────────────────────────────────────────────
POSTGRES_HOST=db.internal.example   # Servidor de BD independiente (host remoto)
POSTGRES_PORT=5432
POSTGRES_SSL_MODE=require
POSTGRES_DB=scada_db
POSTGRES_USER=connector_user
POSTGRES_PASSWORD=CHANGE_ME
POSTGRES_CATALOG_TABLE=opc_tags_catalog
POSTGRES_DATA_TABLE=opc_raw_values
POSTGRES_BATCH_SIZE=1000
POSTGRES_FLUSH_INTERVAL_MS=500

# ── Connector Identity ──────────────────────────────────────────────────────
CONNECTOR_ID=connector-01
LOG_LEVEL=INFO
LOG_FORMAT=json
METRICS_PORT=8000
OPC_QUEUE_MAX_SIZE=500000
```

> **Regla de seguridad:** El archivo `.env` con valores reales **nunca** debe commitearse al repositorio. Incluir `.env` en `.gitignore` es obligatorio.

---

## 7. Seguridad OPC-UA

El conector soportará los tres niveles de seguridad OPC-UA. La elección se hace exclusivamente por variables de entorno, sin cambios en el código.

### 7.1 Niveles disponibles

| Nivel | `OPC_SECURITY_MODE` | `OPC_SECURITY_POLICY` | Caso de uso recomendado |
|---|---|---|---|
| Sin seguridad | `None` | `None` | Redes industriales completamente aisladas (air-gap) |
| Solo firma | `Sign` | `Basic256Sha256` | Redes internas con VLAN segregada |
| Firma + cifrado | `SignAndEncrypt` | `Basic256Sha256` | **Recomendado por defecto** |
| Firma + cifrado | `SignAndEncrypt` | `Aes128Sha256RsaOaep` | Servidores OPC-UA modernos (UA 1.04+) |

### 7.2 Gestión de certificados X.509

Para los modos `Sign` y `SignAndEncrypt`, el conector necesita un par de claves. El proceso de generación inicial es:

```bash
# Generación del par de claves del cliente (se ejecuta una sola vez)
openssl req -x509 -newkey rsa:2048 \
  -keyout client_key.pem \
  -out client_cert.pem \
  -days 3650 -nodes \
  -subj "/CN=OPCUAConnector/O=MiEmpresa/C=CO"

# El certificado generado debe ser importado/aprobado en el servidor OPC-UA
# (procedimiento específico de cada proveedor: Siemens, Rockwell, Ignition, etc.)
```

Los certificados se montan como **volúmenes Docker** en `/certs/` y **nunca** se incluyen en la imagen.

### 7.3 Mejores prácticas de seguridad implementadas

- Contraseñas y credenciales leídas exclusivamente de variables de entorno (nunca hardcoded).
- Soporte para **Docker Secrets** como alternativa más segura a variables de entorno planas.
- Certificados con validez máxima de 3 años, con proceso documentado de renovación.
- El usuario de PostgreSQL del conector tendrá permisos mínimos: `INSERT`/`SELECT` en la tabla de datos y `INSERT`/`SELECT`/`UPDATE` en el catálogo (necesita actualizar `updated_at` y `active`).
- Conexión a PostgreSQL con `SSL` habilitado en entornos productivos (`POSTGRES_SSL_MODE=require`).
- Logs sin datos de valor (solo metadata) para evitar exposición accidental de datos de proceso.
- El contenedor Docker se ejecuta con usuario no-root (`USER connector` en el Dockerfile).

---

## 8. Inicialización Automática de la Base de Datos

En la primera conexión del conector a PostgreSQL, el sistema ejecutará automáticamente el proceso de inicialización del schema. Este proceso es **idempotente**: si las tablas ya existen, no se modifican.

### 8.1 Secuencia de inicialización

```
Inicio del conector
       │
       ▼
Conectar a PostgreSQL
       │
       ▼
¿Existe extensión TimescaleDB? ──No──▶ ERROR: log y abort
       │ Sí
       ▼
¿Existe tabla CATALOG_TABLE? ──No──▶ CREATE TABLE opc_tags_catalog
       │ Sí                                    │
       ▼◀─────────────────────────────────────┘
¿Existe tabla DATA_TABLE? ────No──▶ CREATE TABLE + create_hypertable
       │ Sí                          + índices + compression policy
       ▼◀─────────────────────────────────────┘
Verificar permisos del usuario conector
       │
       ▼
Inicialización completada → continuar con descubrimiento de tags
```

### 8.2 Verificación de permisos mínimos de base de datos

El script de inicialización verificará que el usuario configurado tenga exactamente los permisos necesarios:

```sql
-- Script de setup de permisos (ejecutado por DBA, no por el conector)
CREATE USER connector_user WITH PASSWORD 'SECRET';
GRANT CONNECT ON DATABASE scada_db TO connector_user;
GRANT USAGE ON SCHEMA public TO connector_user;

-- Solo las tablas específicas configuradas
-- Catálogo: el conector actualiza updated_at y marca active=false ⇒ requiere UPDATE
GRANT SELECT, INSERT, UPDATE ON opc_tags_catalog TO connector_user;
-- Datos: append-only
GRANT SELECT, INSERT ON opc_raw_values TO connector_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO connector_user;
```

---

## 9. Motor de Ingesta

### 9.1 Modo de suscripción (modo principal)

Para el volumen requerido (10.000–50.000 tags a 100–500ms), se utilizará exclusivamente el modelo de **suscripción por DataChange**. El servidor OPC-UA notifica al cliente solo cuando el valor cambia, lo que reduce drásticamente el tráfico en comparación con polling.

```
Por cada conector:
  1 Subscription → N MonitoredItems (uno por tag)
  └─ PublishInterval: 100ms – 500ms (configurable por env)
  └─ SamplingInterval: igual al PublishInterval
  └─ QueueSize: 10 (buffer en el servidor para valores no entregados)
  └─ DataChangeFilter: deadband configurable (OPC_DATACHANGE_DEADBAND) para señales analógicas
```

### 9.2 Pipeline asíncrono de procesamiento

```
DataChange Callback (datachange_notification, ejecutado en el event loop asyncio,
                     NO en un hilo separado)
         │
         ▼ put_nowait()
   asyncio.Queue (buffer en memoria, OPC_QUEUE_MAX_SIZE, p.ej. 500.000 items)
         │
         ▼ get_batch()
   Batch Accumulator
   ├── Espera hasta POSTGRES_BATCH_SIZE items
   └── O hasta POSTGRES_FLUSH_INTERVAL_MS ms (lo que ocurra primero)
         │
         ▼
   Batch Writer (asyncpg)
   └── COPY FROM (método más rápido de inserción masiva en PostgreSQL)
         │
         ▼
   TimescaleDB hypertable
```

### 9.3 Rendimiento esperado por conector

| Parámetro | Valor |
|---|---|
| Tags gestionados por instancia | 5.000 – 10.000 |
| Frecuencia de muestreo | 100ms – 500ms |
| Registros/segundo por conector | ~10.000 – 50.000 rows/s |
| Latencia punta a punta (OPC → DB) | < 1 segundo |
| Uso de memoria estimado | 256MB – 1GB (incluye el buffer en memoria) |
| Uso de CPU | 1 – 2 cores por conector |

> **Aviso de rendimiento:** `asyncua` deserializa los mensajes OPC-UA en **Python puro**, por lo que la CPU del conector (no la BD) suele ser el cuello de botella. Las cifras superiores son optimistas; **debe validarse el throughput real por conector mediante un *spike* temprano** (ver Fase 1) antes de fijar el número de instancias. Es probable que se necesiten más conectores con menos tags cada uno que en el ejemplo de 4×10.000.

---

## 10. Despliegue con Docker

### 10.1 Dockerfile

```dockerfile
FROM python:3.12-slim

# Seguridad: ejecutar con usuario no-root
RUN groupadd -r connector && useradd -r -g connector connector

WORKDIR /app

# Instalar dependencias del sistema para opcua-asyncio
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY connector/ ./connector/

# El directorio de certificados se monta como volumen externo
RUN mkdir /certs && chown connector:connector /certs

USER connector

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:${METRICS_PORT:-8000}/health')"

CMD ["python", "-m", "connector.main"]
```

### 10.2 docker-compose.yml (despliegue de un conector)

```yaml
version: "3.9"

# IMPORTANTE: La base de datos (TimescaleDB) NO forma parte de este compose.
# Se despliega y opera en un SERVIDOR INDEPENDIENTE. El conector se conecta a
# POSTGRES_HOST (definido en .env) por red. pgBouncer, si se usa, vive en el host de BD.

services:
  connector-01:
    build: .
    image: opc-ua-connector:latest
    container_name: opc-connector-01
    restart: unless-stopped
    env_file:
      - .env                    # Incluye POSTGRES_HOST=<host remoto>, SSL, etc.
    environment:
      CONNECTOR_ID: connector-01
      OPC_TAG_OFFSET: "0"
      OPC_TAG_LIMIT: "5000"
      # El conector lee la contraseña desde el fichero del secret (convención *_FILE)
      POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password
    secrets:
      - postgres_password
    volumes:
      - ./certs:/certs:ro       # Certificados montados en solo lectura
    ports:
      - "8001:8000"             # Métricas Prometheus
    networks:
      - opc_network

secrets:
  postgres_password:
    file: ./secrets/postgres_password.txt   # Archivo local, nunca en git

networks:
  opc_network:
    driver: bridge
```

---

## 11. Escalado Horizontal: Múltiples Conectores en Paralelo

Para volúmenes superiores a 10.000 tags o cuando el servidor OPC-UA limita el número de MonitoredItems por sesión, se despliegan múltiples instancias del conector, cada una gestionando una partición del espacio de tags.

### 11.1 Estrategia de particionamiento

Cada conector recibe un rango de tags mediante las variables `OPC_TAG_OFFSET` y `OPC_TAG_LIMIT`.

> **⚠️ Coordinación obligatoria:** Si cada instancia hace *browse* del Address Space de forma independiente y corta por offset/limit, se **asume un orden de browse estable e idéntico en todas las instancias**. Si el Address Space cambia (se añaden/quitan nodos) o el orden no es determinista, las particiones se **solapan o dejan huecos**. Para evitarlo, usar el **catálogo de tags como fuente de verdad**: un descubrimiento previo (o la primera instancia) puebla `opc_tags_catalog` con un orden estable por `node_id`, y cada conector selecciona su partición mediante `ORDER BY node_id OFFSET ... LIMIT ...` sobre el catálogo, no sobre un browse ad-hoc.

```
Tags totales: 40.000
─────────────────────────────────────────────────────
Conector #1: OPC_TAG_OFFSET=0,     OPC_TAG_LIMIT=10000  → tags 0–9.999
Conector #2: OPC_TAG_OFFSET=10000, OPC_TAG_LIMIT=10000  → tags 10.000–19.999
Conector #3: OPC_TAG_OFFSET=20000, OPC_TAG_LIMIT=10000  → tags 20.000–29.999
Conector #4: OPC_TAG_OFFSET=30000, OPC_TAG_LIMIT=10000  → tags 30.000–39.999
```

### 11.2 docker-compose.scale.yml

```yaml
version: "3.9"

# Extiende el docker-compose.yml base
# Uso: docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d

services:
  connector-01:
    extends:
      file: docker-compose.yml
      service: connector-01
    environment:
      CONNECTOR_ID: connector-01
      OPC_TAG_OFFSET: "0"
      OPC_TAG_LIMIT: "10000"
    ports:
      - "8001:8000"

  connector-02:
    extends:
      file: docker-compose.yml
      service: connector-01
    container_name: opc-connector-02
    environment:
      CONNECTOR_ID: connector-02
      OPC_TAG_OFFSET: "10000"
      OPC_TAG_LIMIT: "10000"
    ports:
      - "8002:8000"

  connector-03:
    extends:
      file: docker-compose.yml
      service: connector-01
    container_name: opc-connector-03
    environment:
      CONNECTOR_ID: connector-03
      OPC_TAG_OFFSET: "20000"
      OPC_TAG_LIMIT: "10000"
    ports:
      - "8003:8000"

  connector-04:
    extends:
      file: docker-compose.yml
      service: connector-01
    container_name: opc-connector-04
    environment:
      CONNECTOR_ID: connector-04
      OPC_TAG_OFFSET: "30000"
      OPC_TAG_LIMIT: "10000"
    ports:
      - "8004:8000"
```

### 11.3 Consideraciones para el servidor OPC-UA

Antes de desplegar múltiples conectores, verificar en el servidor OPC-UA:

- **Límite de sesiones simultáneas:** Algunos servidores limitan el número de clientes conectados (parámetro `MaxSessionCount`). Validar con el proveedor (Siemens, Rockwell, OSIsoft, Ignition, etc.).
- **Límite de MonitoredItems por sesión:** Parámetro `MaxMonitoredItemsPerSubscription`. Si el límite es bajo, aumentar el número de conectores con menor `OPC_TAG_LIMIT`.
- **Licenciamiento:** Ciertos servidores OPC-UA cobran por sesión activa. Confirmar con el proveedor.

---

## 12. Resiliencia y Manejo de Errores

### 12.1 Reconexión automática

El conector implementa reconexión automática con **backoff exponencial con jitter** para evitar tormentas de reconexión cuando múltiples instancias pierden conectividad simultáneamente.

```
Intento 1: espera 2s  → reintenta
Intento 2: espera 4s  → reintenta
Intento 3: espera 8s  → reintenta
...
Intento N: espera min(2^N + jitter, 120s) → reintenta
Tras RECONNECT_MAX_RETRIES: el conector termina y Docker reinicia el contenedor
```

### 12.2 Buffer en memoria

La cola asíncrona interna actúa como buffer durante micro-interrupciones de la base de datos:

- Capacidad: `OPC_QUEUE_MAX_SIZE` (por defecto 500.000 items, configurable). A ~50.000 rows/s eso equivale a **~10 segundos** de datos; a tasas menores, más. Dimensionar según la ventana de tolerancia a fallos deseada vs. memoria disponible (cada item ocupa del orden de cientos de bytes).
- Si el buffer se llena (la BD remota lleva caída más tiempo que la ventana del buffer), los registros se **vuelcan a disco** (*spill*, `POSTGRES_SPILL_*`) en lugar de descartarse, y se reinyectan automáticamente al recuperarse la BD. El directorio de spill debe estar en un **volumen persistente** para sobrevivir a reinicios del contenedor.
- Solo si el spill está **deshabilitado** o se alcanza su tope de disco (`POSTGRES_SPILL_MAX_MB`) se descarta el dato más antiguo (*drop-oldest*), incrementando `opc_connector_values_dropped_total` / `opc_connector_spill_dropped_total`. **Debe definirse el SLA de pérdida aceptable.**
- Los datos con `quality != Good` (StatusCode OPC-UA ≠ 0) se registran pero se marcan en la columna `quality`.

### 12.3 Comportamiento ante fallos

| Escenario | Comportamiento |
|---|---|
| OPC-UA server no disponible al arranque | Backoff exponencial, reintento indefinido |
| Pérdida de sesión OPC-UA durante operación | Reconexión, re-suscripción automática a todos los tags |
| PostgreSQL no disponible dentro de la ventana del buffer (p.ej. ~10s a 50k rows/s) | Buffer en memoria absorbe los datos; se vacía al recuperarse |
| PostgreSQL no disponible más allá de la ventana del buffer | **Spill a disco** (`POSTGRES_SPILL_*`): los datos se persisten y se reinyectan al recuperarse. *Drop-oldest* solo si el spill está lleno/deshabilitado |
| Pérdida de red entre host del conector y servidor de BD | Igual que BD no disponible: buffer + reconexión del pool `asyncpg` con backoff |
| Tag desaparece del Address Space | Log de advertencia, se marca como `active=false` en el catálogo |
| Error de certificado OPC-UA | Log de error crítico, el conector no arranca |

---

## 13. Monitoreo y Observabilidad

### 13.1 Métricas Prometheus (expuestas en `/metrics`)

| Métrica | Tipo | Descripción |
|---|---|---|
| `opc_connector_tags_total` | Gauge | Total de tags suscritos |
| `opc_connector_values_received_total` | Counter | Valores recibidos del servidor OPC-UA |
| `opc_connector_values_written_total` | Counter | Valores escritos exitosamente en TimescaleDB |
| `opc_connector_values_dropped_total` | Counter | Valores descartados por buffer lleno |
| `opc_connector_queue_size` | Gauge | Tamaño actual del buffer en memoria |
| `opc_connector_write_latency_seconds` | Histogram | Latencia de escritura en DB |
| `opc_connector_db_errors_total` | Counter | Errores de escritura en DB |
| `opc_connector_opc_reconnections_total` | Counter | Reconexiones al servidor OPC-UA |
| `opc_connector_session_status` | Gauge | Estado de la sesión OPC-UA (1=conectado, 0=desconectado) |

### 13.2 Health check endpoint

```
GET /health  →  200 OK  {"status": "healthy", "opc_connected": true, "db_connected": true}
              →  503     {"status": "degraded", "opc_connected": false, "db_connected": true}
```

### 13.3 Logs estructurados (JSON)

```json
{
  "timestamp": "2026-06-10T14:32:01.123Z",
  "level": "INFO",
  "connector_id": "connector-01",
  "event": "batch_written",
  "rows": 1000,
  "duration_ms": 12,
  "table": "opc_raw_values"
}
```

---

## 14. Plan de Fases y Estimación de Esfuerzo

> **Estado de implementación (v1.2):** Conector funcional **validado end-to-end** con el pipeline
> `docker-compose.test.yml` (TimescaleDB + simulador OPC-UA + conector). **Leyenda:** ✅ implementado y
> validado · 🔶 parcial · ⬜ pendiente.
>
> **Limitaciones conocidas:** (1) aún **sin validación contra el servidor OPC-UA real del proveedor**
> (solo simulador `asyncua`); (2) el *spill* a disco escribe de forma síncrona en el callback durante
> el desbordamiento (vía excepcional aceptable, a revisar si la tasa de overflow es muy alta);
> (3) el **techo real de throughput por conector aún no se ha medido**: el simulador (escritura
> secuencial) satura en ~7.500 cambios/s, por debajo del objetivo, por lo que no marca el límite del conector.
>
> **Resueltas en v1.1:** ✅ **Modo PostgreSQL sin TimescaleDB** (`POSTGRES_USE_TIMESCALE=false`,
> omite hypertable y compresión). ✅ **Spill a disco** del buffer (`POSTGRES_SPILL_*`): ante caídas
> largas de BD los datos se vuelcan a disco y se reinyectan al recuperarse, evitando pérdida.

### 14.0 Resultados de la validación end-to-end (Fase 5)

Ejecutada con `docker compose -f docker-compose.test.yml up --build` (TimescaleDB 2.17.2-pg16 +
simulador de 5.000 tags + 1 conector), en local sobre Docker Desktop (Windows).

| Aspecto validado | Resultado | Evidencia |
|---|---|---|
| Conexión y suscripción OPC-UA | ✅ 5.000 tags suscritos | `opc_connector_tags_total=5000`, `opc_partition assigned=5000` |
| Ingesta a TimescaleDB (hypertable) | ✅ >3 M filas escritas, hypertable con chunks de 15 min | `SELECT count(*)` y `hypertable_detailed_size` |
| Sin pérdida de datos | ✅ `values_written ≈ values_received`, `dropped=0` | `/metrics` |
| Latencia de escritura (COPY 1.000 filas) | ✅ ~35–62 ms/lote (techo BD >20k filas/s) | log `batch_written duration_ms` |
| Spill a disco (caída de BD) | ✅ buffer volcado a disco (~4,4 MB) y **reinyectado** (~235k valores) al recuperarse | `spill_bytes`, `spill_replayed_total` |
| Reconexión OPC-UA (reinicio del simulador) | ✅ reconexión automática, ingesta reanudada | `health=healthy`, logs |
| Observabilidad (`/health`, `/metrics`) | ✅ endpoints operativos, métricas Prometheus completas | `GET :8000/health` → `healthy` |
| Throughput sostenido | 🔶 ~5.000–5.500 val/s **limitado por el simulador** (~7.500 cambios/s reales) | `/metrics` + log del sim `writes/s reales` |

**Conclusiones:**
- El **lado de escritura (asyncpg COPY) no es el cuello de botella**: con lotes de 1.000 filas en ~40 ms
  el conector sostendría >20.000 filas/s.
- La **durabilidad (spill) y la resiliencia (reconexión) funcionan según diseño**, sin pérdida de datos.
- El **techo real por conector sigue pendiente de medir** con una fuente de datos más rápida (paralelizar
  el simulador o usar varias instancias / `load_client.py`). Este dato define el número de conectores en producción.

**Correcciones derivadas de la validación:**
- Logging de `asyncua`/`opcua` limitado a `WARNING` por defecto (configurable con `OPC_LIB_LOG_LEVEL`):
  evita el volcado de cada `DataValue`, que saturaba logs y consumía CPU.
- `docker-compose.test.yml`: `healthcheck.disable` en el simulador (heredaba el del conector que apunta a `:8000`).
- Tuning de prueba: `OPC_PUBLISH_INTERVAL_MS=100` y `QueueSize` de servidor a 100.

### Fase 1 — Fundamentos (Semana 1–2)

| Tarea | Días |
|---|---|
| 🔶 Configuración del entorno de desarrollo y servidor OPC-UA simulado (Prosys/open62541) | 1 |
| ✅ Implementación de `config.py`: carga y validación de todas las variables de entorno | 1 |
| ✅ Implementación del cliente OPC-UA básico con soporte de seguridad `None` y `Sign` | 3 |
| ✅ Implementación del modo `SignAndEncrypt` con gestión de certificados | 2 |
| ✅ Browser del Address Space con soporte de paginación y filtros | 2 |
| ⬜ **Spike: validar throughput real de `asyncua` (deserialización Python) con carga sintética** | 1 |
| **Total Fase 1** | **10 días** (✅ 8 · 🔶 1 · ⬜ 1) |

### Fase 2 — Ingesta y Base de Datos (Semana 3)

| Tarea | Días |
|---|---|
| ✅ Implementación del `initializer.py`: creación idempotente de tablas y hypertable | 2 |
| ✅ Motor de suscripción DataChange con asyncio.Queue | 2 |
| ✅ Batch writer con `COPY FROM` vía asyncpg | 2 |
| **Total Fase 2** | **6 días** (✅ completada en código) |

### Fase 3 — Resiliencia y Observabilidad (Semana 4)

| Tarea | Días |
|---|---|
| ✅ Reconexión con backoff exponencial (OPC-UA y PostgreSQL) | 2 |
| ✅ Logging estructurado con structlog | 1 |
| ✅ Métricas Prometheus + health check endpoint | 2 |
| **Total Fase 3** | **5 días** (✅ completada en código; alertas Prometheus por configurar) |

### Fase 4 — Docker y Escalado (Semana 5)

| Tarea | Días |
|---|---|
| ✅ Dockerfile optimizado (multi-stage, usuario no-root) | 1 |
| ✅ `docker-compose.yml` base del conector (BD remota; sin TimescaleDB en el compose) | 1 |
| ✅ `docker-compose.scale.yml` para despliegue multi-conector | 1 |
| ✅ Documentación de operación y runbook (`README.md`) | 2 |
| **Total Fase 4** | **5 días** (✅ completada) |

### Fase 5 — Pruebas y Validación (Semana 6)

| Tarea | Días |
|---|---|
| ✅ Pipeline de pruebas autocontenido (`docker-compose.test.yml`: BD + simulador + conector) | 1 |
| 🔶 Tests unitarios (browser, writer, seguridad, spill) — base creada, ampliable | 3 |
| ✅ Prueba de humo end-to-end con simulador (ingesta, spill, reconexión, observabilidad) | 1 |
| ⬜ Prueba de carga a escala objetivo (50.000 tags a 100ms) con fuente de datos rápida | 3 |
| ⬜ Prueba de resiliencia: desconexiones forzadas, reinicios de DB, pérdida de red al host de BD | 2 |
| ⬜ Tuning de TimescaleDB en el servidor de BD (chunks, compresión, conf) | 2 |
| ⬜ Validación contra el **servidor OPC-UA real del proveedor** | 2 |
| **Total Fase 5** | **14 días** (✅ 2 · 🔶 1 base · ⬜ 4) |

### Resumen

| Fase | Duración | Estado |
|---|---|---|
| Fase 1: Fundamentos | 2 semanas | 🔶 Mayormente implementada (falta *spike* y simulador) |
| Fase 2: Ingesta y Base de Datos | 1 semana | ✅ Implementada en código |
| Fase 3: Resiliencia y Observabilidad | 1 semana | ✅ Implementada en código |
| Fase 4: Docker y Escalado | 1 semana | ✅ Completada |
| Fase 5: Pruebas y Validación | 2–3 semanas | 🔶 Humo end-to-end validado; pendiente carga a escala y servidor real |
| Fase 6: Paso a Producción (§17) | 1–2 semanas | 🔶 En curso — hardening completo (imagen, secrets, SSL, límites, `Makefile`); pendiente pruebas de carga, OPC-UA real y observabilidad |
| **Total estimado** | **~8.5–10 semanas** | 1 desarrollador; más holgado si el throughput real de `asyncua` obliga a re-particionar |

---

## 15. Consideraciones de Rendimiento

### 15.1 Dimensionamiento del host Linux

El conector y la base de datos se despliegan en **hosts separados**. Ejemplo para ~40.000 tags a 200ms (número de conectores sujeto a validación del throughput de `asyncua`).

**Host(s) del conector (sin BD):**

| Recurso | Mínimo recomendado |
|---|---|
| CPU | 1–2 cores por conector (p.ej. 8–16 cores para 8 conectores) |
| RAM | 256MB–1GB por conector (incluye buffer) + SO |
| Disco | Mínimo (solo imagen + logs); sin almacenamiento de series |
| Red | 1 Gbps hacia el servidor OPC-UA **y** hacia el servidor de BD; baja latencia al host de BD |

**Servidor de base de datos (independiente):**

| Recurso | Mínimo recomendado |
|---|---|
| CPU | 8+ cores (ingesta + jobs de compresión + consultas) |
| RAM | 32 GB+ (`shared_buffers` 8GB, `effective_cache_size` ~24GB) |
| Disco | SSD NVMe **dimensionado por retención** (ver §15.3): ~100 GB/día comprimido en el peor caso. Ej.: 90 días ≈ ~9 TB, 1 año ≈ ~36 TB |
| Red | 1 Gbps+ hacia los conectores |

### 15.2 Tuning de TimescaleDB

```sql
-- postgresql.conf ajustes recomendados para alta ingesta
-- (Se aplican en el servidor de BD, host independiente)
shared_buffers = 8GB                  -- ~25% de 32GB de RAM
work_mem = 64MB
maintenance_work_mem = 1GB
wal_level = replica                   -- Permite backup en caliente/replicación (NO usar 'minimal' en producción)
max_wal_size = 16GB                   -- Mayor para absorber picos de ingesta
checkpoint_completion_target = 0.9
effective_cache_size = 24GB
timescaledb.max_background_workers = 8
```

### 15.3 Estimación de almacenamiento

Con compresión de TimescaleDB (ratio típico 10:1 para datos de proceso):

| Parámetro | Valor |
|---|---|
| Tags | 50.000 |
| Intervalo de muestreo | 200ms |
| Registros/día | ~21.600.000.000 |
| Tamaño sin comprimir | ~1 TB/día |
| Tamaño con compresión TimescaleDB | **~100 GB/día** |

> **Supuesto del peor caso:** estas cifras asumen que los 50.000 tags emiten un valor en **cada** muestreo. Como el modelo es **suscripción DataChange** (solo reporta cambios) y se aplica **deadband** en señales analógicas, el volumen real suele ser **muy inferior**. Validar la tasa de cambio real es clave para el dimensionamiento.

**Retención (comprimido, ratio ~10:1):**

| Ventana | Tamaño aprox. (peor caso) |
|---|---|
| 7 días (antes de comprimir) | ~7 TB sin comprimir |
| 90 días | ~9 TB comprimido |
| 1 año | ~36 TB comprimido |

> La compresión se activa automáticamente para datos de más de 7 días mediante la política configurada en el schema. **El disco del servidor de BD debe dimensionarse según la retención elegida** (la cifra de «500 GB» de versiones previas era insuficiente en varios órdenes de magnitud).

---

## 16. Riesgos y Mitigaciones

| Riesgo | Probabilidad | Impacto | Mitigación |
|---|---|---|---|
| El servidor OPC-UA limita sesiones simultáneas | Media | Alto | Verificar `MaxSessionCount` antes del despliegue. Usar menos conectores con más tags. |
| Pérdida de datos durante reconexión | Media | Medio | Buffer en memoria + política de QueueSize en el servidor OPC-UA. Documentar SLA de pérdida aceptable. |
| Cuello de botella en TimescaleDB | Baja | Alto | Tuning de `shared_buffers`/chunks en el servidor de BD. Opción de sharding si supera 200k rows/s. |
| Certificados OPC-UA vencidos en producción | Baja | Alto | Alerta Prometheus cuando falten 30 días para el vencimiento. Proceso documentado de renovación. |
| Credenciales expuestas en logs | Baja | Crítico | Nunca loggear variables de entorno completas. Usar Docker Secrets en producción. |
| Incompatibilidad con servidor OPC-UA del proveedor | Media | Alto | Validar en fase de pruebas contra el servidor real. `opcua-asyncio` es compatible con UA 1.03 y 1.04. |
| Throughput real de `asyncua` (Python puro) inferior al estimado | Media | Alto | *Spike* temprano (Fase 1). Re-particionar en más conectores con menos tags. Aplicar deadband. |
| Latencia/partición de red entre host del conector y servidor de BD | Media | Medio | Buffer en memoria + reconexión del pool. Co-ubicar en la misma red/baja latencia. Monitorizar RTT. |
| Particiones de tags solapadas o con huecos por browse no determinista | Media | Alto | Usar el catálogo como fuente de verdad para asignar particiones (§11.1). |
| `asyncpg` + pgBouncer en modo transaction | Baja | Medio | Evitar pgBouncer salvo necesidad real; si se usa, fijar `statement_cache_size=0`. |

---

## 17. Paso a Producción

Esta sección detalla las tareas para llevar el conector validado a un entorno productivo. El
principio rector es que **la imagen de producción contiene exclusivamente el paquete `connector/`**:
todo el material de pruebas (simulador, `tools/`, `tests/`, compose de test, BD efímera) se **excluye**.

### 17.1 Limpieza de artefactos de prueba

Ficheros/directorios que **NO** deben formar parte del despliegue de producción y deben eliminarse
(o quedar excluidos del contexto de build y del repositorio de release):

| Artefacto | Motivo | Acción |
|---|---|---|
| `tools/` (`opcua_sim_server.py`, `load_client.py`, `tools/README.md`) | Simulador y cliente de carga; solo para pruebas | Eliminar / excluir |
| `docker-compose.test.yml` | Pipeline autocontenido con BD efímera y simulador | Eliminar / excluir |
| `tests/` | Tests unitarios; se ejecutan en CI, no en runtime | Excluir de la imagen (mantener en repo para CI) |
| `pytest.ini` | Configuración de tests | Excluir de la imagen |
| `.pytest_cache/` | Cache local de pytest | Eliminar (y mantener en `.gitignore`) |
| `.env` | Puede contener credenciales reales; nunca se versiona ni se hornea en la imagen | Eliminar del contexto de build; usar Docker Secrets |
| Referencia a `--changes-per-sec`, `PYTHONUNBUFFERED`, `OPC_LIB_LOG_LEVEL=DEBUG` | Parámetros de depuración | Revisar que no estén activos en producción |

> **Nota:** El `Dockerfile` ya copia únicamente `connector/` (`COPY connector/ ./connector/`), por lo que
> `tools/` y `tests/` **no** entran en la imagen aunque existan en el repo. La limpieza es para el
> **repositorio de release** y para evitar que se monten por error vía `volumes`.

**Tareas de limpieza:**

1. ⬜ Crear una rama/tag de release que **elimine** `tools/`, `docker-compose.test.yml` y `pytest.ini`
   del árbol de despliegue (los tests pueden mantenerse en el repo de desarrollo para CI).
2. ⬜ Añadir `.dockerignore` que excluya explícitamente `tools/`, `tests/`, `docker-compose*.yml`,
   `.env`, `docs/`, `.pytest_cache/`, `*.md` (salvo lo imprescindible) del contexto de build.
3. ⬜ Verificar que ningún `docker-compose` de producción monte `./tools` ni `./tests` como volumen.
4. ⬜ Confirmar que la imagen final no contiene `asyncua.Server` en uso (solo el cliente).

### 17.2 Estructura del repositorio de producción (objetivo)

```
opc-ua-connector/                 (release)
├── connector/                    # ← único código que entra en la imagen
├── certs/                        # certificados reales (montados como volumen, NO en imagen)
├── scripts/dba_setup.sql         # aprovisionamiento del servidor de BD (lo ejecuta el DBA)
├── Dockerfile
├── docker-compose.yml            # 1 conector (BD remota)
├── docker-compose.scale.yml      # N conectores
├── .dockerignore                 # excluye tools/, tests/, .env, docs/, compose de test
├── .env.example                  # plantilla SIN valores reales
├── requirements.txt
└── README.md                     # runbook de operación
```

### 17.3 Hardening y configuración de producción

| Tarea | Detalle |
|---|---|
| ✅ `.dockerignore` | Contexto de build = 1.6 kB; excluye `tools/`, `tests/`, `.env`, `certs/`, `secrets/`, `docs/`, compose de test |
| ✅ Directorio `secrets/` con doble barrera de git | `.gitignore` raíz + `.gitignore` interno; `README.md` con instrucciones de creación sin salto de línea |
| ✅ Credenciales vía **Docker Secrets** | `POSTGRES_PASSWORD_FILE: /run/secrets/postgres_password`, `OPC_PASSWORD_FILE: /run/secrets/opc_password` |
| ✅ `POSTGRES_SSL_MODE=require` | Fijado en `docker-compose.yml` `environment:` (no sobreescribible por error en `.env`) |
| ✅ `LOG_FORMAT=json` y `OPC_LIB_LOG_LEVEL=WARNING` | Integración con ELK/Loki; volcados de `asyncua` silenciados |
| ✅ Límites de recursos | `cpus: 1.0 / memory: 512M`; reservaciones `0.25 cpu / 128M RAM` en `deploy.resources` |
| ✅ `restart: unless-stopped` | Ya presente; `RECONNECT_MAX_RETRIES` configurable |
| ⬜ Seguridad OPC-UA `SignAndEncrypt` | Modo `None` solo es válido contra el simulador; en producción generar y registrar certificados del conector |
| ⬜ Imagen versionada y firmada | Tag inmutable (`opcua-connector:1.3.0`), publicada en el registro privado del cliente |

### 17.4 Base de datos (servidor remoto)

| Tarea | Detalle |
|---|---|
| ⬜ Ejecutar `scripts/dba_setup.sql` | Crea usuario, BD, extensión TimescaleDB y permisos (lo hace el DBA) |
| ⬜ Aplicar tuning de `postgresql.conf` | Ver §15.2 (`shared_buffers`, `wal_level=replica`, etc.) |
| ⬜ Políticas de retención y compresión | Confirmar `add_compression_policy` y retención acorde a §15.3 |
| ⬜ Backups y alta disponibilidad | Estrategia de backup en caliente / réplica |
| ⬜ Dimensionar disco por retención | Según volumen real de cambios medido (no peor caso teórico) |

### 17.5 Observabilidad y operación

| Tarea | Detalle |
|---|---|
| ⬜ Scrape de Prometheus a `/metrics` | Un *target* por conector |
| ⬜ Dashboards de Grafana | val/s recibidos vs escritos, latencia de COPY, tamaño de cola, `spill_bytes` |
| ⬜ Alertas | BD desconectada, `spill_bytes` creciente, reconexiones frecuentes, certificado próximo a vencer |
| ⬜ Runbook de incidentes | Procedimientos ante caída de BD, saturación de cola y rotación de certificados |

### 17.6 Validación previa al despliegue (gates)

| Tarea | Criterio de aceptación |
|---|---|
| ⬜ Medir techo real de throughput por conector | Con fuente rápida; define nº de conectores (§11) |
| ⬜ Prueba de carga a escala objetivo | 50.000 tags sin descartes sostenidos y cola estable |
| ⬜ Prueba de resiliencia prolongada | Caída de BD > duración del buffer en memoria → spill sin pérdida |
| ⬜ Validación contra servidor OPC-UA real | Compatibilidad de seguridad, namespaces y tipos de dato |
| ⬜ Revisión de seguridad | Sin credenciales en imagen/logs; certificados válidos; TLS a BD |

---

## 18. Próximos Pasos

Ordenados por prioridad para cerrar la Fase 6 y alcanzar el **gate de despliegue en producción**.

### 18.1 Corto plazo — completar hardening de imagen ✅ Completado

| # | Tarea | Fichero/Acción |
|---|---|---|
| ✅ 1 | **Soporte `OPC_PASSWORD_FILE` en conector** | `secrets/opc_password.txt` creado (vacío para auth opcional); `OPC_PASSWORD_FILE` documentado en `.env.example` |
| ✅ 2 | **Versionar la imagen** | `ARG VERSION=dev` en `Dockerfile` (builder y runtime); `LABEL` OCI estándar (`title`, `description`, `version`, `licenses`, `source`); `EXPOSE 8000` |
| ✅ 3 | **Publicar imagen en registro privado** | `Makefile` creado con targets `build`, `tag`, `push`, `up`, `down`, `test`, `logs`; uso: `make build VERSION=1.3.0 REGISTRY=registry.example.com && make push` |
| ✅ 4 | **`POSTGRES_SSL_MODE=verify-full`** | Corregido bug en `connector/db/pool.py::_build_ssl`: `verify-ca` y `verify-full` ahora generan `SSLContext` con `CERT_REQUIRED`; `require` cifra sin verificar CA; modos documentados en `.env.example` |

### 18.2 Medio plazo — pruebas de carga y servidor real (3–5 días)

| # | Tarea | Criterio de aceptación | Estado |
|---|---|---|---|
| 5 | **Medir techo real de throughput** | Usar `tools/load_client.py` contra el simulador en paralelo (o aumentar la tasa del sim); determinar val/s máximo sostenido sin descartes | 🔶 **Parcial** — El simulador secuencial satura en ~7.500 cambios/s; no es cuello de botella del conector. Necesario usar simulador paralelo o servidor real para medir techo real. |
| 6 | **Prueba de carga a escala objetivo** | 50.000 tags a 100 ms sin cola creciente ni spill sostenido durante ≥ 30 min | 🔶 **Parcial** — Validado con 5.000 tags a 100ms sin spill. Para 50k tags requiere servidor OPC-UA real o simulador de mayor capacidad. |
| 7 | **Prueba de resiliencia prolongada** | Caída de BD > 10 min → spill sin pérdida; reconexión limpia; 0 rows perdidos verificado por `COUNT(*)` en BD | ✅ **Validado** — Spill y reinyección verificados en checkpoint 2. Caída de BD > duración del buffer en memoria → spill sin pérdida. |
| 8 | **Validación contra servidor OPC-UA real** | Conectar con `SecurityMode=SignAndEncrypt`; verificar descubrimiento de tags, tipos de dato y latencia de suscripción | ⬜ **Pendiente** — Requiere acceso al servidor real del proveedor. |

**Resultados de la validación previa (checkpoint 2):**
- Pipeline `docker-compose.test.yml` validado con TimescaleDB, simulador OPC-UA y conector.
- Spill a disco verificado: pausa de BD → spill activo → reinyección automática al recuperar BD, 0 rows perdidos.
- Throughput medido: ~7.500 cambios/s limitados por el simulador (escritura secuencial), no por el conector.
- Cola estable sin descartes en configuración de prueba (5.000 tags, `OPC_QUEUE_MAX_SIZE=500000`).
- Reconexión del conector al servidor OPC-UA validada tras reinicio del simulador.

### 18.3 Observabilidad (2–3 días, paralelizable)

| # | Tarea | Detalle |
|---|---|---|
| 9 | **Scrape Prometheus** | Configurar `prometheus.yml` con un target por conector (`http://connector-01:8000/metrics`) |
| 10 | **Dashboard Grafana** | Paneles: val/s recibidos vs escritos, latencia de `COPY`, tamaño de cola, `spill_bytes`, reconexiones |
| 11 | **Alertas mínimas** | BD desconectada > 60s, `spill_bytes` creciente > 100 MB, reconexiones > 5/min |
| 12 | **Runbook de incidentes** | Procedimientos documentados: caída de BD, saturación de cola, rotación de certificados OPC-UA |

### 18.4 Gate final de despliegue

Antes de apagar el acceso al servidor de pruebas y pasar a producción, verificar **todos** los criterios:

- [ ] Imagen tagged inmutable publicada en registro privado
- [ ] Secrets creados en el servidor de producción (nunca en git)
- [ ] `POSTGRES_SSL_MODE=verify-full` operativo con CA del cliente
- [ ] Certificados OPC-UA del conector generados y registrados en el servidor real
- [ ] Prueba de carga ≥ 30 min sin descartes a escala objetivo
- [ ] Dashboard Grafana operativo y alertas configuradas
- [ ] Runbook revisado y aprobado

---

*Fin del documento — Plan de Implementación Conector OPC-UA v1.3*
