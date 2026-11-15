# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 · Notebook 1 — Capa Bronce: ingesta de clientes
# MAGIC **Producto de datos:** Clientes · **Lee:** `/Volumes/externo/sistema_origen/archivos/clientes.csv` · **Escribe:** `clientes.bronce.clientes`
# MAGIC
# MAGIC Bronce es la **puerta de entrada al lakehouse**: lee el archivo de la **fuente externa** (catálogo `externo`) y lo
# MAGIC guarda en el catálogo `clientes`, schema `bronce`, **tal como llega**: no se limpia ni se descarta nada. Si mañana
# MAGIC descubrimos un error en la limpieza, podemos volver a procesar desde aquí sin pedirle el archivo de nuevo al sistema origen.
# MAGIC
# MAGIC En este notebook solo hay dos pasos: **leer el CSV** y **guardarlo en una tabla Delta**.
# MAGIC
# MAGIC **Prerrequisito:** haber ejecutado `0.Nb_Configuracion` y subido `clientes.csv` al volumen.
# MAGIC
# MAGIC Fuentes oficiales: [Medallion architecture](https://learn.microsoft.com/azure/databricks/lakehouse/medallion) ·
# MAGIC [Read CSV files](https://learn.microsoft.com/azure/databricks/query/formats/csv) ·
# MAGIC [Tutorial: load and transform data](https://learn.microsoft.com/azure/databricks/getting-started/dataframes).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Leer el CSV del volumen
# MAGIC - `spark.read.format("csv")`: lector de archivos CSV.
# MAGIC - `option("header", True)`: la primera fila trae los nombres de las columnas.
# MAGIC - **No** usamos `inferSchema`: así **todas las columnas llegan como texto (`STRING`)**. En bronce no queremos que
# MAGIC   Spark convierta nada; si una fecha viene como `"no registra"`, se guarda tal cual. Los tipos se asignan en plata.
# MAGIC - `F.current_timestamp()`: agregamos la columna `_fecha_ingesta` con el momento de la carga (auditoría).
# MAGIC
# MAGIC **Qué esperar:** `Filas leídas: 123` y una tabla con los datos **sucios**: espacios de más, mayúsculas mezcladas,
# MAGIC identificaciones vacías o con letras, fechas en dos formatos…

# COMMAND ----------

from pyspark.sql import functions as F

ruta_archivo = "/Volumes/externo/sistema_origen/archivos/clientes.csv"

df_bronce = (
    spark.read.format("csv")
         .option("header", True)                                  # primera fila = nombres de columnas
         .load(ruta_archivo)                                      # sin inferSchema: todo llega como STRING
         .withColumn("_fecha_ingesta", F.current_timestamp())     # auditoría: cuándo se cargó
)

print("Filas leídas:", df_bronce.count())
display(df_bronce)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Guardar los datos crudos en una tabla Delta
# MAGIC - `write.format("delta")`: Delta es el formato por defecto de las tablas en Databricks (lo escribimos para que se vea).
# MAGIC - `mode("overwrite")`: reemplaza el contenido de la tabla. Si ejecutas el notebook dos veces, **no duplica** filas.
# MAGIC - `saveAsTable("catálogo.schema.tabla")`: crea la tabla en Unity Catalog (o la reemplaza si ya existe).
# MAGIC
# MAGIC **Qué esperar:** la celda termina sin resultado.

# COMMAND ----------

(df_bronce.write
          .format("delta")
          .mode("overwrite")
          .saveAsTable("clientes.bronce.clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Revisar la tabla de bronce
# MAGIC ### 3.1 · Los datos crudos
# MAGIC **Qué esperar:** 123 filas, todas las columnas de tipo `string` (menos `_fecha_ingesta`).

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE clientes.bronce.clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Un primer vistazo a la calidad
# MAGIC Antes de limpiar hay que **medir**. Solo existen 6 ciudades, 2 estados y 2 géneros… ¿por qué hay tantas formas de escribirlos?
# MAGIC
# MAGIC **Qué esperar:** `filas = 123`, `ciudades_distintas = 18`, `estados_distintos = 5`, `generos_distintos = 9` e
# MAGIC `identificaciones_nulas = 5` (el CSV trae otras 3 con solo espacios, que no cuentan como nulas).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                                  AS filas,
# MAGIC        count(DISTINCT ciudad)                    AS ciudades_distintas,
# MAGIC        count(DISTINCT estado)                    AS estados_distintos,
# MAGIC        count(DISTINCT genero)                    AS generos_distintos,
# MAGIC        count_if(numero_identificacion IS NULL)   AS identificaciones_nulas
# MAGIC FROM clientes.bronce.clientes;

# COMMAND ----------

# MAGIC %md
# MAGIC **Siguiente paso:** abre `2.Nb_Plata_Clientes`.
