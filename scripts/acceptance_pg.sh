#!/usr/bin/env bash
# ============================================================================
# Prueba de aceptación con PostgreSQL/TimescaleDB real y rol restringido.
#
# Levanta un contenedor efímero, aplica scripts/dba_setup.sql (connector_user
# sin superusuario) y ejecuta tests/integration con ese rol.
#
# Uso:
#   ./scripts/acceptance_pg.sh
#   PG_IMAGE=timescale/timescaledb:2.17.2-pg15 ./scripts/acceptance_pg.sh
#   PYTHON=../.venv/Scripts/python.exe ./scripts/acceptance_pg.sh
# ============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PG_IMAGE="${PG_IMAGE:-timescale/timescaledb:2.17.2-pg16}"
PG_CONTAINER="${PG_CONTAINER:-opc-acceptance-pg}"
PG_PORT="${PG_PORT:-55432}"
PG_ADMIN_PASSWORD="${PG_ADMIN_PASSWORD:-admin}"
PG_APP_PASSWORD="${PG_APP_PASSWORD:-acceptance}"
PYTHON="${PYTHON:-python3}"

cleanup() {
  docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker rm -f "$PG_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$PG_CONTAINER" \
  -e POSTGRES_PASSWORD="$PG_ADMIN_PASSWORD" \
  -e POSTGRES_DB=scada_db \
  -p "${PG_PORT}:5432" \
  "$PG_IMAGE" >/dev/null

echo "Esperando a PostgreSQL (${PG_IMAGE})..."
ready=0
for _ in $(seq 1 60); do
  if docker exec "$PG_CONTAINER" pg_isready -U postgres -d scada_db >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
if [ "$ready" -ne 1 ]; then
  echo "ERROR: PostgreSQL no arrancó en 60 s" >&2
  docker logs "$PG_CONTAINER" >&2 || true
  exit 1
fi

echo "Aplicando scripts/dba_setup.sql como DBA..."
sed "s/PASSWORD 'SECRET'/PASSWORD '${PG_APP_PASSWORD}'/" scripts/dba_setup.sql \
  | docker exec -i "$PG_CONTAINER" psql -v ON_ERROR_STOP=1 -U postgres -d scada_db >/dev/null

echo "Ejecutando pruebas de aceptación con connector_user..."
ACCEPTANCE_ADMIN_DSN="postgresql://postgres:${PG_ADMIN_PASSWORD}@localhost:${PG_PORT}/scada_db" \
ACCEPTANCE_CONNECTOR_DSN="postgresql://connector_user:${PG_APP_PASSWORD}@localhost:${PG_PORT}/scada_db" \
PYTHONPATH="$ROOT" \
"$PYTHON" -m pytest tests/integration -q -m integration "$@"
