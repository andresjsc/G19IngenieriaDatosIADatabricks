CREATE TABLE IF NOT EXISTS {catalogo}.plata.clientes (
    NumeroIdentificacion STRING NOT NULL COMMENT 'Número de documento del cliente: solo dígitos, de 8 a 10 caracteres'
  , TipoIdentificacion   STRING          COMMENT 'Tipo de documento: CC (cédula), CE (cédula de extranjería), TI (tarjeta de identidad), PA (pasaporte) o ND (no definido)'
  , Nombres              STRING          COMMENT 'Nombres del cliente con Mayúscula Inicial'
  , Apellidos            STRING          COMMENT 'Apellidos del cliente con Mayúscula Inicial'
  , Email                STRING          COMMENT 'Correo electrónico en minúsculas; null si no era válido'
  , Telefono             STRING          COMMENT 'Número celular de 10 dígitos sin indicativo; null si no era válido'
  , FechaNacimiento      DATE            COMMENT 'Fecha de nacimiento; null si no se pudo interpretar'
  , Ciudad               STRING          COMMENT 'Nombre oficial de la ciudad de residencia'
  , Genero               STRING          COMMENT 'Género: F (femenino), M (masculino) o No informado'
  , FechaRegistro        DATE            COMMENT 'Fecha en que el cliente se registró'
  , Estado               STRING          COMMENT 'Estado del cliente: Activo o Inactivo'
  , FechaIngesta         TIMESTAMP       COMMENT 'Auditoría: momento en que la fila se cargó en bronce'
  , FechaProceso         TIMESTAMP       COMMENT 'Auditoría: momento en que la fila se procesó en plata'
)
CLUSTER BY AUTO
COMMENT 'Clientes limpios, validados y sin duplicados (uno por número de identificación)'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
  , 'delta.autoOptimize.autoCompact'   = 'true'
  , 'delta.enableChangeDataFeed'       = 'true'
  , 'clusterByAuto'                    = 'true'
);