# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 1 · Notebook 2 — Capa Plata: limpieza de clientes
# MAGIC **Producto de datos:** Clientes · **Lee:** `clientes.bronce.clientes` · **Escribe:** `clientes.plata.clientes`
# MAGIC
# MAGIC La capa **plata** convierte los datos crudos en datos **confiables**: con nombres de columna de negocio, tipos
# MAGIC correctos, valores estandarizados y sin clientes inválidos ni duplicados.
# MAGIC
# MAGIC | Paso | Qué hace | Herramienta |
# MAGIC |---|---|---|
# MAGIC | 1 | Crear la tabla de plata (columnas en **PascalCase**, comentarios y propiedades Delta) | Spark SQL |
# MAGIC | 2 | Leer la tabla de bronce | PySpark |
# MAGIC | 3 | Renombrar las columnas de bronce (nombres del CSV) a los nombres de plata | PySpark |
# MAGIC | 4 | Filtrar identificaciones nulas y las que no cumplen el número de dígitos | PySpark |
# MAGIC | 5 | Limpiar nombres, correo, teléfono y tipo de documento | PySpark |
# MAGIC | 6 | Estandarizar ciudad, género y estado, convertir fechas y quitar duplicados | Spark SQL |
# MAGIC | 7 | Insertar los datos en la tabla Delta de plata | `spark.sql` |
# MAGIC | 8 | Verificar el resultado | Spark SQL |
# MAGIC
# MAGIC **Prerrequisito:** haber ejecutado `1.Nb_Bronce_Clientes`.
# MAGIC
# MAGIC Fuentes oficiales: [Medallion architecture](https://learn.microsoft.com/azure/databricks/lakehouse/medallion) ·
# MAGIC [Table properties](https://learn.microsoft.com/azure/databricks/delta/table-properties) ·
# MAGIC [Auto optimize](https://learn.microsoft.com/azure/databricks/delta/tune-file-size) ·
# MAGIC [Change data feed](https://learn.microsoft.com/azure/databricks/delta/delta-change-data-feed) ·
# MAGIC [Liquid clustering](https://learn.microsoft.com/azure/databricks/delta/clustering) ·
# MAGIC [PySpark functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html) ·
# MAGIC [try_to_date](https://learn.microsoft.com/azure/databricks/sql/language-manual/functions/try_to_date) ·
# MAGIC [INSERT](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-dml-insert-into).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Crear la tabla de plata (Spark SQL)
# MAGIC **Primero se define la tabla**, con su contrato: nombres, tipos y descripciones. Después se carga.
# MAGIC - **Nombres en PascalCase** (`NumeroIdentificacion`, `FechaNacimiento`…): en bronce conservamos los nombres del CSV;
# MAGIC   en plata usamos el estándar de nombres del lakehouse.
# MAGIC - **Tipos reales:** `DATE` para las fechas. La identificación sigue siendo `STRING` porque es un código, no un número
# MAGIC   para hacer cuentas.
# MAGIC - `NOT NULL`: Delta rechaza cualquier fila que llegue sin identificación.
# MAGIC - `CLUSTER BY AUTO`: Databricks elige y ajusta solo las columnas de *liquid clustering* según las consultas.
# MAGIC - `TBLPROPERTIES`:
# MAGIC
# MAGIC | Propiedad | Qué hace |
# MAGIC |---|---|
# MAGIC | `delta.autoOptimize.optimizeWrite = true` | Al escribir, junta los datos en menos archivos más grandes (*optimized writes*) |
# MAGIC | `delta.autoOptimize.autoCompact = true` | Después de escribir, compacta los archivos pequeños (*auto compaction*) |
# MAGIC | `delta.enableChangeDataFeed = true` | Registra los cambios fila a fila (CDF, Clase 04); lo usaremos en la carga incremental |
# MAGIC | `clusterByAuto = true` | Deja explícito que la tabla usa *liquid clustering* automático |
# MAGIC
# MAGIC **Qué esperar:** `OK`.

# COMMAND ----------

with open("../sql/plata_ddl.sql", "r", encoding="utf-8") as f:
    ddl_plata = f.read().format(catalogo=catalogo)

spark.sql(ddl_plata)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*CREATE TABLE IF NOT EXISTS clientes.plata.clientes (
# MAGIC   NumeroIdentificacion STRING NOT NULL COMMENT 'Número de documento del cliente: solo dígitos, de 8 a 10 caracteres',
# MAGIC   TipoIdentificacion   STRING    COMMENT 'Tipo de documento: CC (cédula), CE (cédula de extranjería), TI (tarjeta de identidad), PA (pasaporte) o ND (no definido)',
# MAGIC   Nombres              STRING    COMMENT 'Nombres del cliente con Mayúscula Inicial',
# MAGIC   Apellidos            STRING    COMMENT 'Apellidos del cliente con Mayúscula Inicial',
# MAGIC   Email                STRING    COMMENT 'Correo electrónico en minúsculas; null si no era válido',
# MAGIC   Telefono             STRING    COMMENT 'Número celular de 10 dígitos sin indicativo; null si no era válido',
# MAGIC   FechaNacimiento      DATE      COMMENT 'Fecha de nacimiento; null si no se pudo interpretar',
# MAGIC   Ciudad               STRING    COMMENT 'Nombre oficial de la ciudad de residencia',
# MAGIC   Genero               STRING    COMMENT 'Género: F (femenino), M (masculino) o No informado',
# MAGIC   FechaRegistro        DATE      COMMENT 'Fecha en que el cliente se registró',
# MAGIC   Estado               STRING    COMMENT 'Estado del cliente: Activo o Inactivo',
# MAGIC   FechaIngesta         TIMESTAMP COMMENT 'Auditoría: momento en que la fila se cargó en bronce',
# MAGIC   FechaProceso         TIMESTAMP COMMENT 'Auditoría: momento en que la fila se procesó en plata'
# MAGIC )
# MAGIC CLUSTER BY AUTO
# MAGIC COMMENT 'Clientes limpios, validados y sin duplicados (uno por número de identificación)'
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact'   = 'true',
# MAGIC   'delta.enableChangeDataFeed'       = 'true',
# MAGIC   'clusterByAuto'                    = 'true'
# MAGIC );*/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Leer la tabla de bronce (PySpark)
# MAGIC `spark.table("catálogo.schema.tabla")` devuelve la tabla como DataFrame. Los nombres de columna son los del CSV.
# MAGIC
# MAGIC **Qué esperar:** `Filas en bronce: 123` y la lista de columnas originales (`numero_identificacion`, `tipo_identificacion`…).

# COMMAND ----------

from pyspark.sql import functions as F

df_bronce = spark.table("clientes.bronce.clientes")

print("Filas en bronce:", df_bronce.count())
print("Columnas en bronce:", df_bronce.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Renombrar las columnas a los nombres de plata (PySpark)
# MAGIC `withColumnRenamed("nombre_actual", "NombreNuevo")` cambia el nombre de una columna sin tocar los datos.
# MAGIC Encadenamos uno por columna para que se vea claramente **de dónde sale** cada columna de plata.
# MAGIC
# MAGIC **Qué esperar:** la lista de columnas en PascalCase: `NumeroIdentificacion`, `TipoIdentificacion`, `Nombres`…

# COMMAND ----------

df_renombrado = (
    df_bronce
        .withColumnRenamed("numero_identificacion", "NumeroIdentificacion")
        .withColumnRenamed("tipo_identificacion", "TipoIdentificacion")
        .withColumnRenamed("nombres", "Nombres")
        .withColumnRenamed("apellidos", "Apellidos")
        .withColumnRenamed("email", "Email")
        .withColumnRenamed("telefono", "Telefono")
        .withColumnRenamed("fecha_nacimiento", "FechaNacimiento")
        .withColumnRenamed("ciudad", "Ciudad")
        .withColumnRenamed("genero", "Genero")
        .withColumnRenamed("fecha_registro", "FechaRegistro")
        .withColumnRenamed("estado", "Estado")
        .withColumnRenamed("_fecha_ingesta", "FechaIngesta")
)

print("Columnas en plata:", df_renombrado.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Filtrar las identificaciones inválidas (PySpark)
# MAGIC Regla de negocio: el número de identificación es **obligatorio** y debe tener **mínimo 8 dígitos** (y máximo 10).
# MAGIC 1. `F.regexp_replace(col, r"[.\s-]", "")`: quita puntos, espacios (`\s`) y guiones que la gente escribe a mano.
# MAGIC    Así `1.020.345.678` queda `1020345678`, y una identificación de solo espacios queda vacía.
# MAGIC 2. **Filtro de nulos:** `F.col(...).isNotNull()` y distinta de `""` elimina los clientes **sin** identificación.
# MAGIC 3. **Filtro de dígitos:** `rlike("^[0-9]{8,10}$")` deja solo las identificaciones que son **de 8 a 10 dígitos**
# MAGIC    (`^` inicio, `$` fin). Se eliminan las que tienen **menos de 8 dígitos**, más de 10 o letras.
# MAGIC
# MAGIC **Qué esperar:** `Bronce: 123 | Sin nulos: 115 | Con dígitos válidos: 106`.

# COMMAND ----------

display(df_renombrado)

# COMMAND ----------

df_temporal = (
    df_renombrado
        .select(
            F.col("NumeroIdentificacion").alias("NumeroIdentificacion_Original"),
            F.regexp_replace("NumeroIdentificacion", r"[.\s-]", "").alias("NumeroIdentificacion_Limpia")
        )
        .filter(F.col("NumeroIdentificacion_Original") == F.col("NumeroIdentificacion_Limpia"))
)

display(df_temporal)

# COMMAND ----------

df_sin_separadores = df_renombrado.withColumn(
    "NumeroIdentificacion", F.regexp_replace("NumeroIdentificacion", r"[.\s-]", "")
)
display(df_sin_separadores)

# COMMAND ----------

df_sin_nulos = df_sin_separadores.filter(
    F.col("NumeroIdentificacion").isNotNull() & (F.col("NumeroIdentificacion") != "")
)
display(df_sin_nulos)

# COMMAND ----------

df_id_valida = df_sin_nulos.filter(
    F.col("NumeroIdentificacion").rlike(f"^[0-9]{{{MIN_DIGITOS},{MAX_DIGITOS}}}$")
)
display(df_id_valida)

# COMMAND ----------

display(
    df_id_valida.filter(F.col("NumeroIdentificacion") == "10234A5678")
)

# COMMAND ----------

MIN_DIGITOS, MAX_DIGITOS = 8, 10                       # regla de negocio del documento

df_sin_separadores = df_renombrado.withColumn(
    "NumeroIdentificacion", F.regexp_replace("NumeroIdentificacion", r"[.\s-]", "")
)

df_sin_nulos = df_sin_separadores.filter(
    F.col("NumeroIdentificacion").isNotNull() & (F.col("NumeroIdentificacion") != "")
)

df_id_valida = df_sin_nulos.filter(
    F.col("NumeroIdentificacion").rlike(f"^[0-9]{{{MIN_DIGITOS},{MAX_DIGITOS}}}$")
)

print(f"Bronce: {df_renombrado.count()} | Sin nulos: {df_sin_nulos.count()} | Con dígitos válidos: {df_id_valida.count()}")

# COMMAND ----------

display(
    df_id_valida.filter(F.col("NumeroIdentificacion") == "10234A5678")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 · ¿Cuáles se eliminaron por el número de dígitos?
# MAGIC `subtract` devuelve las filas del primer DataFrame que **no** están en el segundo.
# MAGIC
# MAGIC **Qué esperar:** 9 filas: con letras (`N/A`, `SIN DATO`, `10234A5678`…) o con muy pocos o demasiados dígitos
# MAGIC (`12345`, `1234567`, `123456789012`…).

# COMMAND ----------

display(
    df_sin_nulos.select("NumeroIdentificacion", "Nombres", "Apellidos")
                .subtract(df_id_valida.select("NumeroIdentificacion", "Nombres", "Apellidos"))
                .withColumn("Digitos", F.length("NumeroIdentificacion"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Limpiar nombres, correo, teléfono y tipo de documento (PySpark)
# MAGIC Cada `withColumn` **reemplaza** la columna por su versión limpia:
# MAGIC
# MAGIC | Columna | Operación | Ejemplo |
# MAGIC |---|---|---|
# MAGIC | `Nombres`, `Apellidos` | `F.trim` quita espacios a los lados; `F.initcap` pone Mayúscula Inicial | `"  JIMÉNEZ ORTIZ "` → `"Jiménez Ortiz"` |
# MAGIC | `Email` | `F.lower(F.trim(...))`; con `F.when(... .rlike(patrón), ...)` queda `null` si no es `usuario@dominio.ext` | `"ana.gomezgmail.com"` → `null` |
# MAGIC | `Telefono` | `F.regexp_replace(col, r"\D", "")` deja solo dígitos (`\D` = lo que no es dígito); `F.substring(col, -10, 10)` toma los **últimos 10** (quita el `57` de `+57`) | `"+57 300-123-4567"` → `"3001234567"` |
# MAGIC | `TipoIdentificacion` | `F.upper` + quitar puntos y espacios | `"C.C."` → `"CC"` |
# MAGIC
# MAGIC Al final pasamos el DataFrame a SQL con una **vista temporal** (`createOrReplaceTempView`): un nombre para el
# MAGIC DataFrame que solo existe en esta sesión.
# MAGIC
# MAGIC **Qué esperar:** nombres, correos y teléfonos ya limpios; ciudades y fechas todavía sin tocar.

# COMMAND ----------

df_paso_python = (
    df_id_valida
        .withColumn("Nombres", F.initcap(F.trim("Nombres")))
        .withColumn("Apellidos", F.initcap(F.trim("Apellidos")))
        .withColumn("Email", F.lower(F.trim("Email")))
        .withColumn("Email", F.when(F.col("Email").rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"), F.col("Email")))
        .withColumn("Telefono", F.regexp_replace("Telefono", r"\D", ""))       # solo dígitos
        .withColumn("Telefono", F.substring("Telefono", -10, 10))              # últimos 10 dígitos
        .withColumn("Telefono", F.when(F.length("Telefono") == 10, F.col("Telefono")))
        .withColumn("TipoIdentificacion", F.upper(F.regexp_replace("TipoIdentificacion", r"[.\s]", "")))
)

df_paso_python.createOrReplaceTempView("vw_clientes_paso_python")
display(df_paso_python)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Estandarizar, convertir fechas y quitar duplicados (Spark SQL)
# MAGIC Creamos una **vista temporal** con el resultado final. Partes de la consulta:
# MAGIC - **CTE `estandarizado`** (`WITH … AS (…)`): un paso intermedio con nombre.
# MAGIC   - `CASE WHEN … THEN … END`: traduce las variantes a un solo valor. `LIKE 'BOGOT%'` atrapa `Bogota`, `BOGOTÁ`,
# MAGIC     `Bogotá D.C.`…; el resto de ciudades solo necesita `initcap(trim(...))`.
# MAGIC   - `try_to_date(texto, formato)`: convierte texto a fecha; si no puede, devuelve `null` **sin fallar**.
# MAGIC     `coalesce(a, b)` se queda con el primer valor no nulo: así probamos los dos formatos (`2024-11-21` y `21/11/2024`).
# MAGIC   - `row_number() OVER (PARTITION BY … ORDER BY FechaRegistro DESC)`: numera los registros de **cada cliente**
# MAGIC     del más nuevo al más viejo.
# MAGIC - `WHERE n = 1`: se queda con el registro **más reciente** de cada cliente (quita los duplicados).
# MAGIC
# MAGIC > **¿Por qué `try_to_date` y no `to_date`?** El cómputo serverless trabaja en **modo ANSI**: `to_date('no registra')`
# MAGIC > **detiene el notebook con un error**. Las funciones `try_…` devuelven `null` en su lugar.
# MAGIC
# MAGIC **Qué esperar:** la celda crea la vista sin mostrar filas.

# COMMAND ----------

with open("../sql/plata_vw_clientes_limpios.sql", "r", encoding="utf-8") as f:
    sql_vw_limpios = f.read()

spark.sql(sql_vw_limpios)

# COMMAND ----------

# MAGIC %sql
# MAGIC /*CREATE OR REPLACE TEMP VIEW vw_clientes_limpios AS
# MAGIC WITH estandarizado AS (
# MAGIC   SELECT NumeroIdentificacion,
# MAGIC          CASE WHEN TipoIdentificacion IN ('CC', 'CE', 'TI') THEN TipoIdentificacion
# MAGIC               WHEN TipoIdentificacion = 'PASAPORTE'          THEN 'PA'
# MAGIC               ELSE 'ND' END                                                        AS TipoIdentificacion,
# MAGIC          Nombres, Apellidos, Email, Telefono,
# MAGIC          coalesce(try_to_date(trim(FechaNacimiento), 'yyyy-MM-dd'),
# MAGIC                   try_to_date(trim(FechaNacimiento), 'dd/MM/yyyy'))                AS FechaNacimiento,
# MAGIC          CASE WHEN upper(trim(Ciudad)) LIKE 'BOGOT%'    THEN 'Bogotá'
# MAGIC               WHEN upper(trim(Ciudad)) LIKE 'MEDELL%'    THEN 'Medellín'
# MAGIC               WHEN upper(trim(Ciudad)) LIKE 'CARTAGENA%' THEN 'Cartagena'
# MAGIC               ELSE initcap(trim(Ciudad)) END                                       AS Ciudad,
# MAGIC          CASE WHEN upper(trim(Genero)) IN ('F', 'FEMENINO')  THEN 'F'
# MAGIC               WHEN upper(trim(Genero)) IN ('M', 'MASCULINO') THEN 'M'
# MAGIC               ELSE 'No informado' END                                              AS Genero,
# MAGIC          coalesce(try_to_date(trim(FechaRegistro), 'yyyy-MM-dd'),
# MAGIC                   try_to_date(trim(FechaRegistro), 'dd/MM/yyyy'))                  AS FechaRegistro,
# MAGIC          initcap(trim(Estado))                                                     AS Estado,
# MAGIC          FechaIngesta
# MAGIC   FROM vw_clientes_paso_python
# MAGIC )
# MAGIC SELECT * EXCEPT (n)
# MAGIC FROM (
# MAGIC   SELECT *, row_number() OVER (PARTITION BY NumeroIdentificacion ORDER BY FechaRegistro DESC) AS n
# MAGIC   FROM estandarizado
# MAGIC )
# MAGIC WHERE n = 1;                -- solo el registro más reciente de cada cliente*/

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Insertar los datos en la tabla Delta de plata (`spark.sql`)
# MAGIC `spark.sql("…")` ejecuta una sentencia SQL **desde Python**. Es útil cuando el SQL hace parte de un proceso en
# MAGIC Python (por ejemplo, para guardar el resultado o armar el nombre de la tabla con variables).
# MAGIC - `INSERT OVERWRITE <tabla> (columnas…) SELECT …`: inserta las filas **reemplazando** las anteriores, así ejecutar
# MAGIC   el notebook dos veces no duplica clientes (en la Clase 06 lo cambiaremos por `MERGE INTO` para cargar solo lo nuevo).
# MAGIC - Escribimos la **lista de columnas** de la tabla y el `SELECT` en el **mismo orden**: así cada valor cae en su columna.
# MAGIC - `current_timestamp()` llena `FechaProceso` con el momento de la carga.
# MAGIC - `display(resultado)` muestra cuántas filas se insertaron.
# MAGIC
# MAGIC **Qué esperar:** `num_affected_rows = 100` (o `num_inserted_rows = 100`).

# COMMAND ----------

with open("../sql/plata_insert_clientes.sql", "r", encoding="utf-8") as f:
    sql_insert_plata = f.read().format(catalogo=catalogo)

resultado = spark.sql(sql_insert_plata)
display(resultado)

# COMMAND ----------

# resultado = spark.sql("""
#     INSERT OVERWRITE clientes.plata.clientes
#         (NumeroIdentificacion, TipoIdentificacion, Nombres, Apellidos, Email, Telefono,
#          FechaNacimiento, Ciudad, Genero, FechaRegistro, Estado, FechaIngesta, FechaProceso)
#     SELECT NumeroIdentificacion, TipoIdentificacion, Nombres, Apellidos, Email, Telefono,
#            FechaNacimiento, Ciudad, Genero, FechaRegistro, Estado, FechaIngesta,
#            current_timestamp() AS FechaProceso
#     FROM vw_clientes_limpios
# """)

# display(resultado)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · Verificar
# MAGIC ### 8.1 · Filas: bronce vs. plata
# MAGIC **Qué esperar:** `FilasBronce = 123`, `FilasPlata = 100`, `Descartadas = 23` (8 sin identificación, 9 con dígitos
# MAGIC inválidos y 6 duplicados).

# COMMAND ----------

with open("../sql/plata_auditoria_conteos.sql", "r", encoding="utf-8") as f:
    sql_auditoria_plata = f.read().format(catalogo=catalogo)

display(spark.sql(sql_auditoria_plata))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SELECT (SELECT count(*) FROM clientes.bronce.clientes)                                                  AS FilasBronce,
# MAGIC        (SELECT count(*) FROM clientes.plata.clientes)                                                   AS FilasPlata,
# MAGIC        (SELECT count(*) FROM clientes.bronce.clientes) - (SELECT count(*) FROM clientes.plata.clientes) AS Descartadas;*/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 · Antes y después
# MAGIC **Qué esperar:** en bronce hay 18 formas de escribir la ciudad, 5 el estado y 9 el género; en plata quedan
# MAGIC 6 ciudades, 2 estados (`Activo`, `Inactivo`) y 3 géneros (`F`, `M`, `No informado`).

# COMMAND ----------

with open("../sql/plata_auditoria_dominios.sql", "r", encoding="utf-8") as f:
    sql_dominios_plata = f.read().format(catalogo=catalogo)

display(spark.sql(sql_dominios_plata))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SELECT 'bronce' AS Capa, count(DISTINCT ciudad) AS Ciudades, count(DISTINCT estado) AS Estados,
# MAGIC        count(DISTINCT genero) AS Generos
# MAGIC FROM clientes.bronce.clientes
# MAGIC UNION ALL
# MAGIC SELECT 'plata', count(DISTINCT Ciudad), count(DISTINCT Estado), count(DISTINCT Genero)
# MAGIC FROM clientes.plata.clientes;*/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 · Las propiedades de la tabla
# MAGIC `SHOW TBLPROPERTIES` lista las propiedades de la tabla.
# MAGIC
# MAGIC **Qué esperar:** entre otras, `delta.autoOptimize.optimizeWrite`, `delta.autoOptimize.autoCompact`,
# MAGIC `delta.enableChangeDataFeed` y `clusterByAuto`, todas en `true`.

# COMMAND ----------

display(spark.sql(f"SHOW TBLPROPERTIES {catalogo}.plata.clientes"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SHOW TBLPROPERTIES clientes.plata.clientes;*/

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.4 · Los datos limpios
# MAGIC **Qué esperar:** 100 clientes, uno por identificación, con fechas de tipo `date` y valores estandarizados.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {catalogo}.plata.clientes ORDER BY Apellidos, Nombres"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SELECT * FROM clientes.plata.clientes ORDER BY Apellidos, Nombres;*/

# COMMAND ----------

# MAGIC %md
# MAGIC **Siguiente paso:** abre `3.Nb_Oro_Clientes`.