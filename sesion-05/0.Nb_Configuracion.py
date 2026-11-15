# Databricks notebook source
# MAGIC %md
# MAGIC # Lab 1 · Notebook 0 — Configuración: fuente externa y lakehouse
# MAGIC **Curso:** Ingeniería de Datos e IA con Databricks · **Clase 05:** ETL con Spark SQL y PySpark · **Producto de datos:** Clientes
# MAGIC
# MAGIC Este notebook crea los **contenedores** del lab. Se ejecuta **una sola vez** (si lo vuelves a ejecutar no pasa nada:
# MAGIC todo usa `IF NOT EXISTS`).
# MAGIC
# MAGIC El escenario: un **sistema externo** (el CRM de la empresa) nos deja un archivo de clientes. Nosotros **consumimos**
# MAGIC esa fuente y construimos un **lakehouse** con sus tres capas:
# MAGIC
# MAGIC ```
# MAGIC   FUENTE EXTERNA                          LAKEHOUSE DEL PRODUCTO CLIENTES
# MAGIC   catálogo externo                        catálogo clientes
# MAGIC ┌───────────────────────────┐        ┌──────────────────────────────────────────────────┐
# MAGIC │ sistema_origen            │        │  bronce          plata            oro            │
# MAGIC │   └─ volumen archivos     │ ─────► │  datos crudos ─► datos limpios ─► datos listos   │
# MAGIC │        └─ clientes.csv    │        │  (tal cual)      (validados)      para consumir  │
# MAGIC └───────────────────────────┘        └──────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC | Catálogo | Schema | Objetos | Qué representa |
# MAGIC |---|---|---|---|
# MAGIC | `externo` | `sistema_origen` | volumen `archivos` con `clientes.csv` | La **fuente externa**: archivos que entrega otro sistema. No es parte del lakehouse |
# MAGIC | `clientes` | `bronce` | tabla `clientes` | Capa Bronce: los datos **tal como llegan** |
# MAGIC | `clientes` | `plata` | tabla `clientes` | Capa Plata: datos **limpios y validados** |
# MAGIC | `clientes` | `oro` | `dim_clientes` y resúmenes | Capa Oro: datos **refinados, listos para consumir** |
# MAGIC
# MAGIC **¿Por qué separarlos?** La fuente externa y el lakehouse tienen **dueños distintos**: el equipo del sistema origen
# MAGIC deja archivos en `externo`; el equipo de datos es dueño de `clientes` y de sus tres capas. Así los permisos son
# MAGIC claros (el ETL solo **lee** de `externo`) y el producto de datos Clientes queda completo en **un solo catálogo**.
# MAGIC
# MAGIC > En un proyecto real, `externo` apuntaría a un **Azure Data Lake** o un **Blob Storage** de la empresa (con una
# MAGIC > *external location* de Unity Catalog). Aquí lo simulamos con un volumen para poder trabajar en Free Edition.
# MAGIC
# MAGIC **Orden de ejecución del lab:** `0.Nb_Configuracion` → `1.Nb_Bronce_Clientes` → `2.Nb_Plata_Clientes` → `3.Nb_Oro_Clientes`.
# MAGIC
# MAGIC **Antes de empezar:** pulsa **Connect** y elige **Serverless**. Ejecuta las celdas en orden con **Shift + Enter**.
# MAGIC
# MAGIC Fuentes oficiales: [Medallion architecture](https://learn.microsoft.com/azure/databricks/lakehouse/medallion) ·
# MAGIC [CREATE CATALOG](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-catalog) ·
# MAGIC [CREATE SCHEMA](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema) ·
# MAGIC [CREATE VOLUME](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-volume) ·
# MAGIC [External locations](https://learn.microsoft.com/azure/databricks/connect/unity-catalog/cloud-storage/external-locations).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · La fuente externa: catálogo `externo`
# MAGIC - `CREATE CATALOG IF NOT EXISTS externo`: el catálogo que representa los sistemas **fuera** del lakehouse.
# MAGIC - `CREATE SCHEMA … sistema_origen`: un schema por sistema que nos entrega datos (hoy, el CRM).
# MAGIC - `CREATE VOLUME … archivos`: un **volumen** es una carpeta gobernada por Unity Catalog para guardar **archivos**
# MAGIC   (no tablas). Aquí deja el sistema origen el CSV.
# MAGIC - Sin `MANAGED LOCATION`, Databricks usa el almacenamiento por defecto del metastore (en Free Edition es lo único disponible).
# MAGIC
# MAGIC **Qué esperar:** la celda termina sin resultado (`OK`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS externo
# MAGIC   COMMENT 'Fuentes externas al lakehouse: archivos que entregan otros sistemas';
# MAGIC CREATE SCHEMA IF NOT EXISTS externo.sistema_origen
# MAGIC   COMMENT 'Sistema origen (CRM) que entrega el archivo de clientes';
# MAGIC CREATE VOLUME IF NOT EXISTS externo.sistema_origen.archivos
# MAGIC   COMMENT 'Zona de llegada de los archivos del sistema origen';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El lakehouse: catálogo `clientes` con sus tres capas
# MAGIC **Un catálogo por producto de datos, un schema por capa.** El nombre completo de una tabla será
# MAGIC `clientes.<capa>.<tabla>`, por ejemplo `clientes.plata.clientes`.
# MAGIC
# MAGIC **Qué esperar:** `OK`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS clientes
# MAGIC   COMMENT 'Lakehouse del producto de datos Clientes (arquitectura medallón)';
# MAGIC CREATE SCHEMA IF NOT EXISTS clientes.bronce COMMENT 'Capa Bronce: datos crudos, tal como llegan de la fuente externa';
# MAGIC CREATE SCHEMA IF NOT EXISTS clientes.plata  COMMENT 'Capa Plata: datos limpios, validados y sin duplicados';
# MAGIC CREATE SCHEMA IF NOT EXISTS clientes.oro    COMMENT 'Capa Oro: datos refinados y listos para el consumo del negocio';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Sube el archivo de clientes a la fuente externa (interfaz)
# MAGIC Hacemos de cuenta que somos el sistema origen entregando su archivo:
# MAGIC 1. Descarga `clientes.csv` del **Classroom** del curso a tu computador.
# MAGIC 2. Barra lateral **Catalog** → `externo` → `sistema_origen` → **Volumes** → `archivos`.
# MAGIC 3. **Upload to this volume** → **arrastra** `clientes.csv` desde la carpeta donde lo descargaste → **Upload**.
# MAGIC
# MAGIC El archivo debe quedar en `/Volumes/externo/sistema_origen/archivos/clientes.csv`.
# MAGIC
# MAGIC ## 4 · Comprobar
# MAGIC - `system.information_schema.schemata`: vista con todos los schemas que puedes ver.
# MAGIC - `LIST '<ruta>'`: lista los archivos de un volumen (en serverless `%fs` no funciona; se usa `LIST` o `dbutils.fs.ls`).
# MAGIC
# MAGIC **Qué esperar:** la primera consulta devuelve 4 filas (`externo.sistema_origen`, `clientes.bronce`, `clientes.plata`,
# MAGIC `clientes.oro`); la segunda, una fila con `clientes.csv` (unos 15 KB).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT catalog_name, schema_name, comment
# MAGIC FROM system.information_schema.schemata
# MAGIC WHERE (catalog_name = 'externo'  AND schema_name = 'sistema_origen')
# MAGIC    OR (catalog_name = 'clientes' AND schema_name IN ('bronce', 'plata', 'oro'))
# MAGIC ORDER BY CASE schema_name WHEN 'sistema_origen' THEN 0 WHEN 'bronce' THEN 1 WHEN 'plata' THEN 2 ELSE 3 END;

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/externo/sistema_origen/archivos/';

# COMMAND ----------

# MAGIC %md
# MAGIC **Siguiente paso:** abre `1.Nb_Bronce_Clientes`.
