-- =============================================================================
-- ARCHIVO: sql/00_setup_ddl.sql
-- DESCRIPCIÓN: Reinicio controlado del esquema, volumen de llegada y tablas
--              Delta para el flujo incremental (Bronce, Plata SCD1/SCD2 y Oro)
-- =============================================================================

DROP SCHEMA IF EXISTS {catalogo}.{esquema} CASCADE;

CREATE SCHEMA {catalogo}.{esquema}
COMMENT 'Ejemplo de carga incremental (Clase 07)';

USE {catalogo}.{esquema};

CREATE VOLUME archivos
COMMENT 'Carpeta donde el CRM deja sus entregas';

-- 1. Capa Bronce: acumula todas las entregas crudas
CREATE TABLE {catalogo}.{esquema}.bronce_clientes (
      entrega       INT
    , id            INT
    , nombre        STRING
    , ciudad        STRING
    , fecha_cambio  DATE
    , archivo       STRING
    , fecha_ingesta TIMESTAMP
)
USING DELTA
COMMENT 'Tabla Bronce: historial acumulado de archivos ingresados con COPY INTO';

-- 2. Capa Plata SCD1: foto actual sin historial
CREATE TABLE {catalogo}.{esquema}.clientes_scd1 (
      id            INT
    , nombre        STRING
    , ciudad        STRING
    , fecha_cambio  DATE
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Tabla Plata SCD Tipo 1: estado vigente por cliente';

-- 3. Capa Plata SCD2: historial completo de vigencias
CREATE TABLE {catalogo}.{esquema}.clientes_scd2 (
      id            INT
    , nombre        STRING
    , ciudad        STRING
    , fecha_inicio  DATE
    , fecha_fin     DATE
    , es_actual     BOOLEAN
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
COMMENT 'Tabla Plata SCD Tipo 2: trazabilidad histórica de cambios por cliente';

-- 4. Capa Oro: métricas filtradas para consumo analítico
CREATE TABLE {catalogo}.{esquema}.oro_clientes_por_ciudad (
      ciudad              STRING
    , clientes_vigentes   BIGINT
    , fecha_calculo       TIMESTAMP
)
USING DELTA
COMMENT 'Tabla Oro: distribución actual de clientes vigentes por ciudad';