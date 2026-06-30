-- ============================================================================
-- Aprovisionamiento del SERVIDOR DE BASE DE DATOS (remoto, independiente).
-- Ejecutado por el DBA UNA sola vez. El conector NO crea extensiones ni usuarios.
-- ============================================================================

-- 1. Extensión TimescaleDB (requiere privilegios de superusuario)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- 2. Usuario de aplicación con permisos mínimos
--    Sustituir 'SECRET' por la contraseña real (o gestionar vía secret manager).
CREATE USER connector_user WITH PASSWORD 'SECRET';

GRANT CONNECT ON DATABASE scada_db TO connector_user;
GRANT USAGE ON SCHEMA public TO connector_user;

-- 3. Permisos sobre las tablas configuradas
--    Catálogo: el conector actualiza updated_at y marca active=false => requiere UPDATE.
--    (Las tablas las crea el conector de forma idempotente en su primera conexión;
--     conceder los permisos por adelantado con DEFAULT PRIVILEGES o tras la creación.)
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE ON TABLES TO connector_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO connector_user;

-- Si las tablas ya existen, conceder explícitamente:
-- GRANT SELECT, INSERT, UPDATE ON opc_tags_catalog TO connector_user;
-- GRANT SELECT, INSERT ON opc_raw_values TO connector_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO connector_user;

-- 4. (Opcional) Tuning recomendado para alta ingesta — ver §15.2 del plan.
--    Aplicar en postgresql.conf y reiniciar:
--      shared_buffers = 8GB
--      effective_cache_size = 24GB
--      wal_level = replica          -- NO usar 'minimal' en producción
--      max_wal_size = 16GB
--      timescaledb.max_background_workers = 8
