SELECT 'bronce'                 AS Capa
     , count(DISTINCT ciudad)   AS Ciudades
     , count(DISTINCT estado)   AS Estados
     , count(DISTINCT genero)   AS Generos
  FROM {catalogo}.bronce.clientes
UNION ALL
SELECT 'plata'                  AS Capa
     , count(DISTINCT Ciudad)   AS Ciudades
     , count(DISTINCT Estado)   AS Estados
     , count(DISTINCT Genero)   AS Generos
  FROM {catalogo}.plata.clientes;