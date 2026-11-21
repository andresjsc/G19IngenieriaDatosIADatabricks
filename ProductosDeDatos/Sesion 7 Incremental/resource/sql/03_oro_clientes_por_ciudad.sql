-- =============================================================================
-- ARCHIVO: sql/03_oro_clientes_por_ciudad.sql
-- DESCRIPCIÓN: Recalcula de forma idempotente la tabla agregada Oro de clientes
--              vigentes por ciudad a partir de clientes_scd1 (foto actual).
-- =============================================================================

INSERT OVERWRITE {catalogo}.{esquema}.oro_clientes_por_ciudad
SELECT
      ciudad
    , COUNT(*)            AS clientes_vigentes
    , CURRENT_TIMESTAMP() AS fecha_calculo
FROM {catalogo}.{esquema}.clientes_scd1
GROUP BY ciudad
ORDER BY clientes_vigentes DESC, ciudad ASC;