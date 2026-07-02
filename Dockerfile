# syntax=docker/dockerfile:1

# Versión de la imagen. Pasar en build-time:
#   docker build --build-arg VERSION=1.3.0 -t opcua-connector:1.3.0 .
ARG VERSION=dev

# ── Builder: compila dependencias en un entorno aislado ──────────────────────
FROM python:3.12-slim AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip wheel --no-cache-dir --wheel-dir /build/wheels -r requirements.txt

# ── Runtime: imagen final mínima, sin toolchain de compilación ───────────────
FROM python:3.12-slim AS runtime

# Reutilizar ARG en el stage runtime
ARG VERSION=dev

# Metadatos OCI estándar (inspeccionables con `docker inspect <image>`)
LABEL org.opencontainers.image.title="opcua-connector" \
      org.opencontainers.image.description="OPC-UA → TimescaleDB ingestion connector" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.source="https://github.com/joluben/opcua-postgres"

# Seguridad: ejecutar con usuario no-root
RUN groupadd -r connector && useradd -r -g connector connector

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/wheels /wheels
COPY requirements.txt .
RUN pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels

COPY connector/ ./connector/

# El directorio de certificados se monta como volumen externo (solo lectura)
RUN mkdir /certs && chown connector:connector /certs

# Directorio del buffer de spill. Se crea con el propietario correcto para que,
# al montar un volumen vacío, Docker herede esta propiedad (usuario no-root).
RUN mkdir -p /var/lib/connector/spill && chown -R connector:connector /var/lib/connector

USER connector

# Puerto de métricas Prometheus y endpoint /health
EXPOSE 8000

# Health check contra el endpoint HTTP del propio conector
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import urllib.request,os; urllib.request.urlopen('http://localhost:%s/health' % os.getenv('METRICS_PORT','8000'))"

CMD ["python", "-m", "connector.main"]
