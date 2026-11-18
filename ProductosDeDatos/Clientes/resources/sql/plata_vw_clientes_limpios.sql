CREATE OR REPLACE TEMP VIEW vw_clientes_limpios AS
WITH estandarizado AS (
    SELECT NumeroIdentificacion
         , CASE WHEN TipoIdentificacion IN ('CC', 'CE', 'TI') THEN TipoIdentificacion
                WHEN TipoIdentificacion = 'PASAPORTE'          THEN 'PA'
                ELSE 'ND' END                                                        AS TipoIdentificacion
         , Nombres
         , Apellidos
         , Email
         , Telefono
         , coalesce(try_to_date(trim(FechaNacimiento), 'yyyy-MM-dd'),
                    try_to_date(trim(FechaNacimiento), 'dd/MM/yyyy'))                AS FechaNacimiento
         , CASE WHEN upper(trim(Ciudad)) LIKE 'BOGOT%'    THEN 'Bogotá'
                WHEN upper(trim(Ciudad)) LIKE 'MEDELL%'    THEN 'Medellín'
                WHEN upper(trim(Ciudad)) LIKE 'CARTAGENA%' THEN 'Cartagena'
                ELSE initcap(trim(Ciudad)) END                                       AS Ciudad
         , CASE WHEN upper(trim(Genero)) IN ('F', 'FEMENINO')  THEN 'F'
                WHEN upper(trim(Genero)) IN ('M', 'MASCULINO') THEN 'M'
                ELSE 'No informado' END                                              AS Genero
         , coalesce(try_to_date(trim(FechaRegistro), 'yyyy-MM-dd'),
                    try_to_date(trim(FechaRegistro), 'dd/MM/yyyy'))                  AS FechaRegistro
         , initcap(trim(Estado))                                                     AS Estado
         , FechaIngesta
      FROM vw_clientes_paso_python
)
SELECT * EXCEPT (n)
  FROM (
    SELECT *
         , row_number() OVER (PARTITION BY NumeroIdentificacion ORDER BY FechaRegistro DESC) AS n
      FROM estandarizado
  )
 WHERE n = 1;                -- solo el registro más reciente de cada cliente