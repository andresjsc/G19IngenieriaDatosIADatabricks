-- =============================================================================
-- ARCHIVO: sql/01_bronce_copy_into.sql
-- DESCRIPCIÓN: Ingesta incremental e idempotente desde el volumen de entregas
--              hacia la tabla Delta bronce_clientes usando COPY INTO.
-- =============================================================================

COPY INTO {catalogo}.{esquema}.bronce_clientes
FROM (
    SELECT
          CAST(entrega AS INT)          AS entrega
        , CAST(id AS INT)               AS id
        , nombre
        , ciudad
        , CAST(fecha_cambio AS DATE)    AS fecha_cambio
        , _metadata.file_name           AS archivo
        , current_timestamp()           AS createAt
    FROM '/Volumes/{catalogo}/{esquema}/archivos/'
)
FILEFORMAT = CSV
PATTERN = 'entrega_*.csv'
FORMAT_OPTIONS (
      'header' = 'true'
    , 'inferSchema' = 'false'
);