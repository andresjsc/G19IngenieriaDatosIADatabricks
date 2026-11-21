-- =============================================================================
-- ARCHIVO: sql/02_plata_merge_scd1.sql
-- DESCRIPCIÓN: Carga incremental hacia clientes_scd1 (foto actual sin historial)
--              usando marca de agua dinámica (MAX(fecha_cambio)) + QUALIFY.
-- =============================================================================

WITH marca_agua AS (
    SELECT COALESCE(MAX(fecha_cambio), DATE '1900-01-01') AS ultima_fecha
    FROM {catalogo}.{esquema}.clientes_scd1
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
MERGE INTO {catalogo}.{esquema}.clientes_scd1 AS t
USING lote_incremental AS s
ON t.id = s.id
WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN
    UPDATE SET
          t.nombre       = s.nombre
        , t.ciudad       = s.ciudad
        , t.fecha_cambio = s.fecha_cambio
WHEN NOT MATCHED THEN
    INSERT (
          id
        , nombre
        , ciudad
        , fecha_cambio
    )
    VALUES (
          s.id
        , s.nombre
        , s.ciudad
        , s.fecha_cambio
    );