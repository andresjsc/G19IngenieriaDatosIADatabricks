-- =============================================================================
-- ARCHIVO: sql/02_plata_merge_scd2.sql
-- DESCRIPCIÓN: Carga incremental SCD Tipo 2 hacia clientes_scd2 usando marca de
--              agua dinámica para cerrar vigencias anteriores e insertar nuevas.
-- =============================================================================

-- Paso 1: Cerrar registros vigentes que sufrieron cambios en nombre o ciudad
WITH marca_agua AS (
    SELECT COALESCE(MAX(fecha_inicio), DATE '1900-01-01') AS ultima_fecha
    FROM {catalogo}.{esquema}.clientes_scd2
)
, lote_incremental AS (
    SELECT
          b.id
        , b.nombre
        , b.ciudad
        , b.fecha_cambio
    FROM {catalogo}.{esquema}.bronce_clientes b
    CROSS JOIN marca_agua m
    WHERE b.fecha_cambio >= m.ultima_fecha
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.id
        ORDER BY b.entrega DESC, b.fecha_cambio DESC, b.createAt DESC
    ) = 1
)
MERGE INTO {catalogo}.{esquema}.clientes_scd2 AS t
USING lote_incremental AS s
ON t.id = s.id
   AND t.es_actual = true
WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN
    UPDATE SET
          t.fecha_fin = s.fecha_cambio
        , t.es_actual   = false;

-- Paso 2: Insertar nuevos clientes y nuevas versiones vigentes de los modificados
WITH marca_agua AS (
    SELECT COALESCE(MAX(fecha_inicio), DATE '1900-01-01') AS ultima_fecha
    FROM {catalogo}.{esquema}.clientes_scd2
)
, lote_incremental AS (
    SELECT
          b.id
        , b.nombre
        , b.ciudad
        , b.fecha_cambio
    FROM {catalogo}.{esquema}.bronce_clientes b
    CROSS JOIN marca_agua m
    WHERE b.fecha_cambio >= m.ultima_fecha
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY b.id
        ORDER BY b.entrega DESC, b.fecha_cambio DESC, b.createAt DESC
    ) = 1
)
INSERT INTO {catalogo}.{esquema}.clientes_scd2 (
      id
    , nombre
    , ciudad
    , fecha_inicio
    , fecha_fin
    , es_actual
)
SELECT
      s.id
    , s.nombre
    , s.ciudad
    , s.fecha_cambio    AS fecha_inicio
    , CAST(NULL AS DATE) AS fecha_fin
    , true              AS es_actual
FROM lote_incremental s
LEFT JOIN {catalogo}.{esquema}.clientes_scd2 t
    ON t.id = s.id
   AND t.es_actual = true
WHERE t.id IS NULL;