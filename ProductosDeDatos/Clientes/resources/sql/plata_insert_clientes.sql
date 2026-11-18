INSERT OVERWRITE {catalogo}.plata.clientes (
    NumeroIdentificacion
  , TipoIdentificacion
  , Nombres
  , Apellidos
  , Email
  , Telefono
  , FechaNacimiento
  , Ciudad
  , Genero
  , FechaRegistro
  , Estado
  , FechaIngesta
  , FechaProceso
)
SELECT NumeroIdentificacion
     , TipoIdentificacion
     , Nombres
     , Apellidos
     , Email
     , Telefono
     , FechaNacimiento
     , Ciudad
     , Genero
     , FechaRegistro
     , Estado
     , FechaIngesta
     , current_timestamp() AS FechaProceso
  FROM vw_clientes_limpios;