# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 · Notebook 4 — Reiniciar el escenario
# MAGIC **Producto de datos:** Clientes · **Borra:** catálogo `clientes` (lakehouse) y, si lo pides, catálogo `externo` (fuente externa)
# MAGIC
# MAGIC Este notebook deja el workspace **como antes del lab**, para volver a ejecutarlo desde cero.
# MAGIC
# MAGIC | Opción `alcance` | Qué borra | Para volver a empezar… |
# MAGIC |---|---|---|
# MAGIC | `lakehouse` | Catálogo `clientes` con sus schemas `bronce`, `plata` y `oro` y todas sus tablas | Ejecuta `0` → `1` → `2` → `3`. **El CSV sigue en el volumen**, no hay que subirlo de nuevo |
# MAGIC | `todo` | Lo anterior **+** catálogo `externo` con el volumen y el archivo `clientes.csv` | Ejecuta `0`, **sube otra vez** `clientes.csv` y sigue con `1` → `2` → `3` |
# MAGIC
# MAGIC > **Cuidado:** `DROP CATALOG … CASCADE` borra el catálogo **con todo lo que tiene adentro** y no se puede deshacer
# MAGIC > (las tablas y volúmenes gestionados se eliminan de forma permanente). Por eso el notebook **no borra nada** hasta
# MAGIC > que escribas `SI` en el widget `confirmar`.
# MAGIC
# MAGIC Fuentes oficiales: [DROP CATALOG](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-drop-catalog) ·
# MAGIC [Databricks widgets](https://learn.microsoft.com/azure/databricks/notebooks/widgets).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Elegir qué borrar (widgets)
# MAGIC Los **widgets** son controles que aparecen **arriba del notebook**. Esta celda crea dos:
# MAGIC - `alcance`: lista desplegable con `lakehouse` o `todo`.
# MAGIC - `confirmar`: cuadro de texto. Escribe **`SI`** (en mayúsculas) para autorizar el borrado.
# MAGIC
# MAGIC `dbutils.widgets.get("nombre")` lee el valor que elegiste.
# MAGIC
# MAGIC **Qué esperar:** aparecen los dos widgets arriba y se imprime lo que se va a borrar. Cambia los valores en los widgets
# MAGIC y **vuelve a ejecutar** esta celda.

# COMMAND ----------

dbutils.widgets.dropdown("alcance", "lakehouse", ["lakehouse", "todo"], "Qué borrar")
dbutils.widgets.text("confirmar", "NO", "Escribe SI para confirmar")

alcance = dbutils.widgets.get("alcance")
confirmar = dbutils.widgets.get("confirmar").strip().upper()

catalogos_a_borrar = ["clientes"] if alcance == "lakehouse" else ["clientes", "externo"]

print("Alcance:", alcance)
print("Catálogos que se borrarán:", ", ".join(catalogos_a_borrar))
print("Confirmado:", "sí" if confirmar == "SI" else "no (escribe SI en el widget 'confirmar')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Qué hay antes de borrar
# MAGIC Consultamos `system.information_schema` para ver los schemas, tablas y volúmenes del lab.
# MAGIC
# MAGIC **Qué esperar:** con el lab completo, 5 tablas en `clientes` (1 en bronce, 1 en plata, 3 en oro) y 1 volumen en `externo`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_catalog AS Catalogo, table_schema AS Schema, table_name AS Objeto, 'tabla' AS Tipo
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog IN ('clientes', 'externo') AND table_schema <> 'information_schema'
# MAGIC UNION ALL
# MAGIC SELECT volume_catalog, volume_schema, volume_name, 'volumen'
# MAGIC FROM system.information_schema.volumes
# MAGIC WHERE volume_catalog IN ('clientes', 'externo')
# MAGIC ORDER BY Catalogo, Schema, Objeto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Borrar
# MAGIC - Si `confirmar` no es `SI`, la celda **se detiene con un error** a propósito y no borra nada.
# MAGIC - `DROP CATALOG IF EXISTS <catálogo> CASCADE`:
# MAGIC   - `IF EXISTS`: no falla si el catálogo ya no existe (puedes ejecutar el notebook dos veces).
# MAGIC   - `CASCADE`: borra también los schemas, tablas y volúmenes que contiene. Sin `CASCADE`, Databricks se niega a
# MAGIC     borrar un catálogo que no está vacío.
# MAGIC - Lo ejecutamos con `spark.sql` desde Python para poder recorrer la lista de catálogos con un `for`.
# MAGIC
# MAGIC **Qué esperar:** `Borrado: clientes` (y `Borrado: externo` si elegiste `todo`).

# COMMAND ----------

if confirmar != "SI":
    raise Exception("No se borró nada: escribe SI en el widget 'confirmar' y vuelve a ejecutar desde la celda del paso 1.")

for catalogo in catalogos_a_borrar:
    spark.sql(f"DROP CATALOG IF EXISTS {catalogo} CASCADE")
    print("Borrado:", catalogo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Comprobar
# MAGIC `SHOW CATALOGS LIKE 'patrón'` lista los catálogos cuyo nombre cumple el patrón (`|` significa «o»).
# MAGIC
# MAGIC **Qué esperar:** con `lakehouse`, solo queda `externo`; con `todo`, la consulta no devuelve filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'clientes|externo';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Volver a empezar
# MAGIC 1. Vuelve a poner `confirmar` en `NO` para no borrar por accidente la próxima vez.
# MAGIC 2. Ejecuta `0.Nb_Configuracion`.
# MAGIC 3. Si elegiste `todo`, sube de nuevo `clientes.csv` a `/Volumes/externo/sistema_origen/archivos/`.
# MAGIC 4. Ejecuta `1.Nb_Bronce_Clientes` → `2.Nb_Plata_Clientes` → `3.Nb_Oro_Clientes`.
