# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 1 · Notebook 5 — Reiniciar el escenario
# MAGIC Borra lo que creó el lab en Databricks. **No** borra nada en Azure (para eso, el README, paso 7).
# MAGIC
# MAGIC | `alcance` | Qué borra | Qué pasa al volver a ejecutar |
# MAGIC |---|---|---|
# MAGIC | `plata_oro` | Schemas `plata` y `oro`, **incluido el checkpoint de plata** | Ejecuta `0` y luego `2` → `3`: plata vuelve a procesar **todo bronce** desde el principio. Sirve para ver para qué está el checkpoint |
# MAGIC | `todo` | Catálogo `iot_sensores` completo (bronce, plata, oro y los dos checkpoints) | Ejecuta `0` → `1` → `2` → `3`: bronce vuelve a leer el event hub desde `earliest` (solo los mensajes que el event hub todavía conserva) |
# MAGIC
# MAGIC > **Cuidado:** `DROP … CASCADE` borra todo lo que contiene y no se puede deshacer. El notebook no borra nada hasta
# MAGIC > que escribas `SI` en el widget `confirmar`.
# MAGIC
# MAGIC Fuentes oficiales: [DROP CATALOG](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-drop-catalog) ·
# MAGIC [DROP SCHEMA](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-drop-schema).

# COMMAND ----------

dbutils.widgets.dropdown("alcance", "plata_oro", ["plata_oro", "todo"], "Qué borrar")
dbutils.widgets.text("confirmar", "NO", "Escribe SI para confirmar")

alcance   = dbutils.widgets.get("alcance")
confirmar = dbutils.widgets.get("confirmar").strip().upper()
print("Alcance:", alcance, "| Confirmado:", confirmar == "SI")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Borrar
# MAGIC **Qué esperar:** `Borrado: …` por cada objeto.

# COMMAND ----------

if confirmar != "SI":
    raise Exception("No se borró nada: escribe SI en el widget 'confirmar' y vuelve a ejecutar.")

if alcance == "todo":
    spark.sql("DROP CATALOG IF EXISTS iot_sensores CASCADE")
    print("Borrado: catálogo iot_sensores")
else:
    for schema in ["iot_sensores.plata", "iot_sensores.oro"]:
        spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        print("Borrado:", schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'iot_sensores';

# COMMAND ----------

# MAGIC %md
# MAGIC Vuelve a poner `confirmar` en `NO` y ejecuta `0.Nb_Configuracion`.