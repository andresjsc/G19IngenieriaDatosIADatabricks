# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 · Notebook 3 — Capa Oro: datos refinados listos para consumir
# MAGIC **Producto de datos:** Clientes · **Lee:** `clientes.plata.clientes` · **Escribe:** 3 tablas en `oro.clientes`
# MAGIC
# MAGIC La capa **oro** se diseña pensando en **quien consume** los datos (analistas, tableros, modelos de IA): plata nos
# MAGIC entrega datos **limpios**; oro les agrega los **cálculos de negocio** para que nadie tenga que hacerlos de nuevo.
# MAGIC
# MAGIC | Tabla | Una fila por… | Cálculos |
# MAGIC |---|---|---|
# MAGIC | `clientes.oro.dim_clientes` | cliente | edad, rango de edad, antigüedad, segmento de antigüedad, dominio y tipo de correo, nivel de contacto |
# MAGIC | `clientes.oro.resumen_clientes_ciudad` | ciudad | totales, % de activos, edad y antigüedad promedio, % contactables |
# MAGIC | `clientes.oro.resumen_clientes_segmento` | rango de edad + género | clientes, activos y activos contactables por correo |
# MAGIC
# MAGIC La capa está en **Spark SQL**: `CREATE OR REPLACE TABLE … AS SELECT` (CTAS) con CTE, como en la Clase 02.
# MAGIC `CREATE OR REPLACE` permite ejecutar el notebook las veces que quieras.
# MAGIC
# MAGIC **Prerrequisito:** haber ejecutado `2.Nb_Plata_Clientes`.
# MAGIC
# MAGIC Fuentes oficiales: [Medallion architecture](https://learn.microsoft.com/azure/databricks/lakehouse/medallion) ·
# MAGIC [CREATE TABLE AS SELECT](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using) ·
# MAGIC [months_between](https://learn.microsoft.com/azure/databricks/sql/language-manual/functions/months_between) ·
# MAGIC [substring_index](https://learn.microsoft.com/azure/databricks/sql/language-manual/functions/substring_index) ·
# MAGIC [Data lineage](https://learn.microsoft.com/azure/databricks/data-governance/unity-catalog/data-lineage) ·
# MAGIC [Visualizations](https://learn.microsoft.com/azure/databricks/visualizations/).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Dimensión de clientes con los cálculos de negocio
# MAGIC La consulta tiene **dos pasos** (CTE `calculos`) para no repetir fórmulas:
# MAGIC
# MAGIC **Paso 1 — calcular los valores base**
# MAGIC - **Edad:** `months_between(hoy, nacimiento) / 12` da los años con decimales; `floor` se queda con los cumplidos.
# MAGIC - **Antigüedad en meses:** `months_between(hoy, FechaRegistro)`, también con `floor`.
# MAGIC - **Dominio del correo:** `substring_index(Email, '@', -1)` devuelve lo que hay **después** de la `@`.
# MAGIC
# MAGIC **Paso 2 — clasificar con `CASE WHEN`**
# MAGIC - **Rango de edad:** `18 a 25`, `26 a 35`, `36 a 45`, `46 a 60`, `Más de 60` o `Sin dato`.
# MAGIC - **Segmento de antigüedad:** `Nuevo` (menos de 12 meses), `En crecimiento` (12 a 35) o `Fiel` (36 o más).
# MAGIC - **Tipo de correo:** `Personal` si el dominio es gmail, hotmail, outlook o yahoo; si no, `Corporativo`.
# MAGIC - **Nivel de contacto:** `Completo` (correo y teléfono), `Parcial` (uno de los dos) o `Sin contacto`.
# MAGIC - **Indicadores `BOOLEAN`** (`EsActivo`, `TieneEmail`…): facilitan filtros y conteos en los tableros.
# MAGIC
# MAGIC **Qué esperar:** `num_affected_rows = 100` (o `num_inserted_rows = 100`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE clientes.oro.dim_clientes
# MAGIC COMMENT 'Dimensión de clientes refinada: una fila por cliente, con Edad, antigüedad, segmentos e indicadores'
# MAGIC TBLPROPERTIES ('capa' = 'oro', 'producto' = 'clientes')
# MAGIC AS
# MAGIC WITH calculos AS (                                   -- paso 1: valores base
# MAGIC   SELECT *,
# MAGIC          CAST(floor(months_between(current_date(), FechaNacimiento) / 12) AS INT) AS Edad,
# MAGIC          CAST(floor(months_between(current_date(), FechaRegistro)) AS INT)        AS AntiguedadMeses,
# MAGIC          substring_index(Email, '@', -1)                                          AS DominioEmail
# MAGIC   FROM clientes.plata.clientes
# MAGIC )
# MAGIC SELECT NumeroIdentificacion                         AS IdCliente,      -- paso 2: clasificar
# MAGIC        TipoIdentificacion,
# MAGIC        concat_ws(' ', Nombres, Apellidos)            AS NombreCompleto,
# MAGIC        Genero, Ciudad, FechaNacimiento, Edad,
# MAGIC        CASE WHEN Edad IS NULL THEN 'Sin dato'
# MAGIC             WHEN Edad <= 25   THEN '18 a 25'
# MAGIC             WHEN Edad <= 35   THEN '26 a 35'
# MAGIC             WHEN Edad <= 45   THEN '36 a 45'
# MAGIC             WHEN Edad <= 60   THEN '46 a 60'
# MAGIC             ELSE                   'Más de 60' END   AS RangoEdad,
# MAGIC        FechaRegistro, AntiguedadMeses,
# MAGIC        round(AntiguedadMeses / 12, 1)               AS AntiguedadAnios,
# MAGIC        CASE WHEN AntiguedadMeses < 12 THEN 'Nuevo'
# MAGIC             WHEN AntiguedadMeses < 36 THEN 'En crecimiento'
# MAGIC             ELSE                            'Fiel' END AS SegmentoAntiguedad,
# MAGIC        Estado,
# MAGIC        Estado = 'Activo'                             AS EsActivo,
# MAGIC        Email, DominioEmail,
# MAGIC        CASE WHEN Email IS NULL THEN NULL
# MAGIC             WHEN DominioEmail IN ('gmail.com', 'hotmail.com', 'outlook.com', 'yahoo.com') THEN 'Personal'
# MAGIC             ELSE 'Corporativo' END                   AS TipoEmail,
# MAGIC        Telefono,
# MAGIC        Email IS NOT NULL                             AS TieneEmail,
# MAGIC        Telefono IS NOT NULL                          AS TieneTelefono,
# MAGIC        CASE WHEN Email IS NOT NULL AND Telefono IS NOT NULL THEN 'Completo'
# MAGIC             WHEN Email IS NOT NULL OR  Telefono IS NOT NULL THEN 'Parcial'
# MAGIC             ELSE 'Sin contacto' END                  AS NivelContacto,
# MAGIC        current_timestamp()                           AS FechaActualizacion
# MAGIC FROM calculos;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 · Revisar la dimensión
# MAGIC **Qué esperar:** 100 clientes con las columnas calculadas. La edad y la antigüedad dependen del día en que ejecutes
# MAGIC el notebook (se calculan con `current_date()`).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT IdCliente, NombreCompleto, Ciudad, Edad, RangoEdad, AntiguedadMeses, SegmentoAntiguedad,
# MAGIC        DominioEmail, TipoEmail, NivelContacto, EsActivo
# MAGIC FROM clientes.oro.dim_clientes
# MAGIC ORDER BY NombreCompleto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Resumen por ciudad
# MAGIC Se construye **sobre la dimensión de oro**, no sobre plata: así las reglas de negocio (qué es «activo», cómo se
# MAGIC calcula la edad) viven en **un solo lugar**.
# MAGIC - `count_if(condición)`: cuenta las filas donde la condición es verdadera.
# MAGIC - `100 * count_if(...) / count(*)`: porcentaje. `round(x, 1)` lo deja con 1 decimal.
# MAGIC
# MAGIC **Qué esperar:** 6 filas, una por ciudad.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE clientes.oro.resumen_clientes_ciudad
# MAGIC COMMENT 'Clientes por Ciudad: totales, % de activos, Edad y antigüedad promedio, % contactables'
# MAGIC TBLPROPERTIES ('capa' = 'oro', 'producto' = 'clientes')
# MAGIC AS
# MAGIC SELECT Ciudad,
# MAGIC        count(*)                                                   AS TotalClientes,
# MAGIC        count_if(EsActivo)                                        AS ClientesActivos,
# MAGIC        count_if(NOT EsActivo)                                    AS ClientesInactivos,
# MAGIC        round(100 * count_if(EsActivo) / count(*), 1)             AS PctActivos,
# MAGIC        round(avg(Edad), 1)                                        AS EdadPromedio,
# MAGIC        round(avg(AntiguedadMeses), 1)                            AS AntiguedadPromedioMeses,
# MAGIC        round(100 * count_if(TieneEmail) / count(*), 1)           AS PctConEmail,
# MAGIC        round(100 * count_if(NivelContacto = 'Completo') / count(*), 1) AS PctContactoCompleto
# MAGIC FROM clientes.oro.dim_clientes
# MAGIC GROUP BY Ciudad;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Resumen por segmento (rango de edad y género)
# MAGIC **Qué esperar:** una fila por cada combinación de rango de edad y género que exista en los datos.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE clientes.oro.resumen_clientes_segmento
# MAGIC COMMENT 'Clientes por rango de Edad y género, para campañas de mercadeo'
# MAGIC TBLPROPERTIES ('capa' = 'oro', 'producto' = 'clientes')
# MAGIC AS
# MAGIC SELECT RangoEdad,
# MAGIC        Genero,
# MAGIC        count(*)                              AS TotalClientes,
# MAGIC        count_if(EsActivo)                   AS ClientesActivos,
# MAGIC        count_if(EsActivo AND TieneEmail)   AS ActivosContactablesPorEmail,
# MAGIC        round(avg(AntiguedadMeses), 1)       AS AntiguedadPromedioMeses
# MAGIC FROM clientes.oro.dim_clientes
# MAGIC GROUP BY RangoEdad, Genero;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Consumir la capa oro
# MAGIC ### 4.1 · ¿Dónde están nuestros clientes?
# MAGIC Consulta directa, sin limpiar ni calcular nada. Para verla como gráfico: en el resultado pulsa **+ → Visualization →
# MAGIC Bar**, eje X `Ciudad`, eje Y `ClientesActivos` y `ClientesInactivos` (apilados).
# MAGIC
# MAGIC **Qué esperar** (la edad y la antigüedad promedio dependen de la fecha de ejecución):
# MAGIC
# MAGIC | Ciudad | TotalClientes | ClientesActivos | ClientesInactivos | PctActivos | PctConEmail |
# MAGIC |---|---|---|---|---|---|
# MAGIC | Barranquilla | 26 | 21 | 5 | 80.8 | 96.2 |
# MAGIC | Cali | 19 | 14 | 5 | 73.7 | 84.2 |
# MAGIC | Bogotá | 16 | 13 | 3 | 81.3 | 87.5 |
# MAGIC | Cartagena | 16 | 8 | 8 | 50.0 | 75.0 |
# MAGIC | Medellín | 12 | 10 | 2 | 83.3 | 100.0 |
# MAGIC | Bucaramanga | 11 | 9 | 2 | 81.8 | 90.9 |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM clientes.oro.resumen_clientes_ciudad
# MAGIC ORDER BY TotalClientes DESC, Ciudad;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · Pregunta de negocio: ¿a quién le enviamos la campaña de correo?
# MAGIC Mercadeo quiere los clientes **activos**, con **correo**, de **26 a 45 años** y que **no sean nuevos**. En oro es
# MAGIC un simple `WHERE`: los cálculos ya están hechos.
# MAGIC
# MAGIC **Qué esperar:** la lista de clientes, ordenada por ciudad y nombre.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT IdCliente, NombreCompleto, Email, TipoEmail, Ciudad, Edad, SegmentoAntiguedad
# MAGIC FROM clientes.oro.dim_clientes
# MAGIC WHERE EsActivo
# MAGIC   AND TieneEmail
# MAGIC   AND RangoEdad IN ('26 a 35', '36 a 45')
# MAGIC   AND SegmentoAntiguedad <> 'Nuevo'
# MAGIC ORDER BY Ciudad, NombreCompleto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Verificar la capa oro
# MAGIC La dimensión debe tener **los mismos clientes que plata** y los resúmenes deben **sumar lo mismo** que la dimensión.
# MAGIC
# MAGIC **Qué esperar:** una fila con `ClientesPlata = 100`, `ClientesDim = 100`, `SumaPorCiudad = 100`, `SumaPorSegmento = 100`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT (SELECT count(*)           FROM clientes.plata.clientes)                AS ClientesPlata,
# MAGIC        (SELECT count(*)           FROM clientes.oro.dim_clientes)              AS ClientesDim,
# MAGIC        (SELECT sum(TotalClientes) FROM clientes.oro.resumen_clientes_ciudad)   AS SumaPorCiudad,
# MAGIC        (SELECT sum(TotalClientes) FROM clientes.oro.resumen_clientes_segmento) AS SumaPorSegmento;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · El recorrido completo
# MAGIC ### 6.1 · Filas por capa
# MAGIC **Qué esperar:** bronce 123 → plata 100 → oro 100 clientes en 6 ciudades.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronce' AS Capa, 'clientes' AS Tabla, count(*) AS Filas FROM clientes.bronce.clientes
# MAGIC UNION ALL SELECT 'plata', 'clientes',                  count(*) FROM clientes.plata.clientes
# MAGIC UNION ALL SELECT 'oro',   'dim_clientes',              count(*) FROM clientes.oro.dim_clientes
# MAGIC UNION ALL SELECT 'oro',   'resumen_clientes_ciudad',   count(*) FROM clientes.oro.resumen_clientes_ciudad
# MAGIC UNION ALL SELECT 'oro',   'resumen_clientes_segmento', count(*) FROM clientes.oro.resumen_clientes_segmento;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 · Linaje (interfaz)
# MAGIC Unity Catalog registra **automáticamente** de dónde sale cada tabla.
# MAGIC 1. Barra lateral **Catalog** → `clientes` → `oro` → `resumen_clientes_ciudad`.
# MAGIC 2. Pestaña **Lineage** → **See lineage graph**.
# MAGIC 3. Deberías ver el camino completo: `clientes.bronce.clientes` → `clientes.plata.clientes` → `clientes.oro.dim_clientes`
# MAGIC    → `clientes.oro.resumen_clientes_ciudad`.
# MAGIC
# MAGIC ## 7 · Cierre del lab
# MAGIC Guarda tu trabajo en el repositorio: desde la carpeta Git de Databricks haz **commit y push** de los 4 notebooks.
# MAGIC
# MAGIC ### Retos
# MAGIC 1. Cambia la regla de identificación en plata a **exactamente 10 dígitos** y vuelve a ejecutar plata y oro.
# MAGIC    ¿Cuántos clientes quedan?
# MAGIC 2. Agrega en plata la ciudad `"Santa Marta"`: ¿necesitas un `WHEN` nuevo o basta con `initcap(trim(...))`?
# MAGIC 3. Crea en oro la tabla `resumen_registros_anio` con los clientes nuevos por año de registro (`year(FechaRegistro)`).
# MAGIC 4. Agrega a `dim_clientes` la columna `MesCumpleanios` (`month(FechaNacimiento)`) y cuenta cuántos clientes
# MAGIC    activos cumplen años este mes.
# MAGIC
# MAGIC ### Reiniciar el escenario
# MAGIC Para volver a hacer el lab desde cero, ejecuta `4.Nb_Reiniciar_Lab`.
