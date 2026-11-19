# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Carga incremental en SQL: `COPY INTO`, `MERGE`, SCD1 y SCD2
# MAGIC **Clase 07 · Carga incremental de datos** · Notebook de ejemplo (todo en SQL)
# MAGIC
# MAGIC **La historia:** un CRM nos manda, cada día, un archivo con los clientes **nuevos** y los que **cambiaron**.
# MAGIC Son 3 archivos pequeños (máximo 5 filas), en la carpeta `datos/` de este ejemplo. **Los subes tú** al volumen,
# MAGIC uno por uno, en el paso del notebook que lo indique:
# MAGIC
# MAGIC | Archivo | Día | Qué trae |
# MAGIC |---|---|---|
# MAGIC | `entrega_1.csv` | 2026-11-02 | Ana (Bogotá), Luis (Cali), Marta (Medellín) — la carga inicial |
# MAGIC | `entrega_2.csv` | 2026-11-03 | Luis se muda a **Barranquilla** · Pedro es **nuevo** · Ana llega **sin cambios** |
# MAGIC | `entrega_3.csv` | 2026-11-04 | Luis se muda **otra vez**, a Bogotá · Sofía es **nueva** |
# MAGIC
# MAGIC | Sección | Qué aprendes |
# MAGIC |---|---|
# MAGIC | 1 | El problema: qué pasa con `INSERT INTO` (duplica) y con `INSERT OVERWRITE` (borra) |
# MAGIC | 2 | `COPY INTO`: subir los 3 archivos uno por uno y ver que cada uno se carga **una sola vez** |
# MAGIC | 3 | Preparar el recorrido completo: vaciar el volumen otra vez y crear bronce, SCD1 y SCD2 |
# MAGIC | 4 · 5 · 6 | **Una sección por entrega:** subir el CSV → `COPY INTO` → `MERGE` SCD1 → `MERGE` SCD2 → ver el resultado |
# MAGIC | 7 | Lo que solo permite SCD2 (consultar el pasado) y comparación final |
# MAGIC
# MAGIC **Requisitos:** Unity Catalog y cómputo **Serverless** (pulsa **Connect → Serverless**). Usa el catálogo
# MAGIC `workspace` de Databricks Free Edition; en otro workspace cámbialo por un catálogo donde puedas crear schemas.
# MAGIC Ejecuta las celdas **en orden**, de arriba hacia abajo. Casi todo es SQL; la única celda en Python es la del
# MAGIC borrado de los CSV del volumen (pasos 2.1 y 3.1).
# MAGIC
# MAGIC Fuentes oficiales: [COPY INTO](https://learn.microsoft.com/azure/databricks/sql/language-manual/delta-copy-into) ·
# MAGIC [read_files](https://learn.microsoft.com/azure/databricks/sql/language-manual/functions/read_files) ·
# MAGIC [INSERT](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-dml-insert-into) ·
# MAGIC [MERGE INTO](https://learn.microsoft.com/azure/databricks/sql/language-manual/delta-merge-into) ·
# MAGIC [SCD tipo 1 y 2](https://learn.microsoft.com/azure/databricks/ldp/cdc) ·
# MAGIC [Work with files in volumes](https://learn.microsoft.com/azure/databricks/volumes/volume-files) ·
# MAGIC [dbutils.fs](https://learn.microsoft.com/azure/databricks/dev-tools/databricks-utils).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Preparación
# MAGIC Creamos un schema solo para este ejemplo y un **volumen** (`archivos`), que hará de carpeta del CRM. La primera
# MAGIC línea borra el schema si ya existía: así el notebook siempre empieza desde cero (si lo vuelves a ejecutar, también se
# MAGIC borran los archivos subidos y tendrás que subirlos otra vez).
# MAGIC
# MAGIC **Qué esperar:** `OK`.

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS workspace.demo_incremental CASCADE;
# MAGIC CREATE SCHEMA workspace.demo_incremental COMMENT 'Ejemplo de carga incremental (Clase 07)';
# MAGIC USE workspace.demo_incremental;
# MAGIC CREATE VOLUME archivos COMMENT 'Carpeta donde el CRM deja sus entregas';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · El problema: ¿cómo cargamos la entrega del día 2?
# MAGIC ### 1.1 · Llega la entrega del día 1
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_1.csv`** (carpeta `datos/` de este ejemplo) → **Upload**.
# MAGIC
# MAGIC Después ejecuta la celda. `read_files` lee el archivo como una tabla (las columnas salen del encabezado).
# MAGIC
# MAGIC **Qué esperar:** 3 filas: Ana (Bogotá), Luis (Cali) y Marta (Medellín). La última columna, `_rescued_data`, la
# MAGIC agrega `read_files` para guardar datos que no encajen en el esquema; aquí sale vacía.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('/Volumes/workspace/demo_incremental/archivos/entrega_1.csv', format => 'csv', header => true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 · Cargamos el día 1 en dos tablas iguales
# MAGIC Vamos a probar dos formas de cargar el día 2, así que dejamos el día 1 en **dos** tablas:
# MAGIC `clientes_insert_into` y `clientes_insert_overwrite`.
# MAGIC
# MAGIC **Qué esperar:** las dos tablas con las mismas 3 filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE clientes_insert_into      (id INT, nombre STRING, ciudad STRING);
# MAGIC CREATE OR REPLACE TABLE clientes_insert_overwrite (id INT, nombre STRING, ciudad STRING);
# MAGIC
# MAGIC INSERT INTO clientes_insert_into      SELECT id, nombre, ciudad FROM read_files('/Volumes/workspace/demo_incremental/archivos/entrega_1.csv', format => 'csv', header => true);
# MAGIC INSERT INTO clientes_insert_overwrite SELECT id, nombre, ciudad FROM read_files('/Volumes/workspace/demo_incremental/archivos/entrega_1.csv', format => 'csv', header => true);
# MAGIC
# MAGIC SELECT 'insert_into' AS tabla, * FROM clientes_insert_into
# MAGIC UNION ALL
# MAGIC SELECT 'insert_overwrite', * FROM clientes_insert_overwrite
# MAGIC ORDER BY tabla, id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 · Llega la entrega del día 2
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_2.csv`** (carpeta `datos/` de este ejemplo) → **Upload**.
# MAGIC
# MAGIC **Qué esperar:** 3 filas: Luis se mudó a **Barranquilla**, Pedro es **nuevo** y Ana viene **otra vez, sin cambios**.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('/Volumes/workspace/demo_incremental/archivos/entrega_2.csv', format => 'csv', header => true);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 · Opción A: `INSERT INTO` (agregar)
# MAGIC `INSERT INTO` **agrega** las filas del día 2 a las que ya estaban.
# MAGIC
# MAGIC **Qué esperar:** **6** filas. **Ana aparece dos veces** y **Luis también** (en Cali y en Barranquilla). ¿Dónde vive
# MAGIC Luis? ¿Tenemos 6 clientes o 4? La tabla ya no sirve para responder.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO clientes_insert_into SELECT id, nombre, ciudad FROM read_files('/Volumes/workspace/demo_incremental/archivos/entrega_2.csv', format => 'csv', header => true);
# MAGIC
# MAGIC SELECT * FROM clientes_insert_into ORDER BY id, ciudad;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 · Opción B: `INSERT OVERWRITE` (reemplazar)
# MAGIC `INSERT OVERWRITE` **borra** lo que había y deja solo las filas del día 2. Es lo que hacía la carga completa de la
# MAGIC Clase 05.
# MAGIC
# MAGIC **Qué esperar:** **3** filas. Luis está bien (Barranquilla) y Pedro entró, pero **Marta desapareció**: no venía en el
# MAGIC archivo de hoy porque no cambió.

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT OVERWRITE clientes_insert_overwrite SELECT id, nombre, ciudad FROM read_files('/Volumes/workspace/demo_incremental/archivos/entrega_2.csv', format => 'csv', header => true);
# MAGIC
# MAGIC SELECT * FROM clientes_insert_overwrite ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.6 · Ninguna de las dos sirve
# MAGIC | | Filas | Problema |
# MAGIC |---|---|---|
# MAGIC | `INSERT INTO` | 6 | **Duplica**: Ana y Luis aparecen dos veces |
# MAGIC | `INSERT OVERWRITE` | 3 | **Pierde** a Marta, que no venía en la entrega |
# MAGIC | Lo que queremos | 4 | Ana, Luis en **Barranquilla**, Marta y Pedro: uno por cliente |
# MAGIC
# MAGIC **Las dos preguntas de toda carga incremental:**
# MAGIC 1. **¿Qué llegó desde la última vez?** → no volver a leer ni duplicar lo que ya se cargó. Lo resuelve
# MAGIC    **`COPY INTO`**.
# MAGIC 2. **¿Qué hago con un registro que ya existía?** → actualizarlo o guardar su historia, sin duplicar ni borrar a
# MAGIC    los demás. Lo resuelve **`MERGE`**, con **SCD1** o **SCD2**.
# MAGIC
# MAGIC En la sección 2 vemos `COPY INTO` solo y, en las secciones 4, 5 y 6, el recorrido completo entrega por entrega.
# MAGIC
# MAGIC > `INSERT OVERWRITE` (la **carga completa**) solo funciona si el origen manda **todos** los datos cada vez. Aquí el
# MAGIC > CRM solo manda lo que cambió.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · `COPY INTO`: cargar cada archivo una sola vez
# MAGIC ### 2.1 · Empezar con el volumen vacío
# MAGIC Para ver cómo `COPY INTO` carga **cada archivo nuevo**, borramos los CSV que subimos en la sección 1 y los
# MAGIC volvemos a subir **uno por uno**, ejecutando `COPY INTO` después de cada uno.
# MAGIC
# MAGIC Esta celda es **Python** (`%python`): la sentencia `REMOVE` de SQL no funciona en notebooks, así que usamos
# MAGIC `dbutils.fs.ls` (listar) y `dbutils.fs.rm` (borrar). También puedes borrarlos a mano: en el volumen, marca los
# MAGIC archivos → **Delete**.
# MAGIC
# MAGIC **Qué esperar:** `Borrado: entrega_1.csv` y `Borrado: entrega_2.csv`, y `LIST` no devuelve filas.

# COMMAND ----------

carpeta = "/Volumes/workspace/demo_incremental/archivos/"
for archivo in dbutils.fs.ls(carpeta):
    if archivo.name.endswith(".csv"):
        dbutils.fs.rm(archivo.path)
        print("Borrado:", archivo.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/workspace/demo_incremental/archivos/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 · Entrega 1 → `COPY INTO`
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_1.csv`** (carpeta `datos/` de este ejemplo) → **Upload**.
# MAGIC
# MAGIC Luego ejecuta la celda. `COPY INTO` carga en una tabla Delta los archivos de una carpeta y **recuerda cuáles ya
# MAGIC cargó**: *«Los archivos de la ubicación de origen que ya se cargaron se omiten»*.
# MAGIC
# MAGIC | Parte | Qué hace |
# MAGIC |---|---|
# MAGIC | `FROM (SELECT … FROM '<carpeta>')` | Lee la carpeta y elige/convierte columnas. `_metadata.file_name` = nombre del archivo |
# MAGIC | `FILEFORMAT = CSV` | Formato de los archivos |
# MAGIC | `PATTERN = 'entrega_*.csv'` | *Glob* que dice qué archivos tomar (`*` = cualquier texto) |
# MAGIC | `FORMAT_OPTIONS ('header' = 'true')` | Opciones del lector: la primera fila trae los nombres de las columnas |
# MAGIC
# MAGIC **Qué esperar:** `num_affected_rows = 3`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronce_copy_into (entrega INT, id INT, nombre STRING, ciudad STRING, fecha_cambio DATE, archivo STRING);
# MAGIC
# MAGIC COPY INTO bronce_copy_into
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 · Ejecutarlo otra vez no duplica
# MAGIC Es la **misma** sentencia, sin subir nada nuevo. Como `entrega_1.csv` ya se cargó, `COPY INTO` se lo salta: es
# MAGIC **idempotente** (ejecutarlo una o diez veces deja el mismo resultado). Compáralo con el `INSERT INTO` del paso 1.4.
# MAGIC
# MAGIC **Qué esperar:** `num_affected_rows = 0` y la tabla sigue con 3 filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronce_copy_into
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 · Entrega 2 → `COPY INTO`
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_2.csv`** (carpeta `datos/` de este ejemplo) → **Upload**.
# MAGIC
# MAGIC Luego ejecuta la celda. **Qué esperar:** `num_affected_rows = 3`: solo carga `entrega_2.csv`, aunque en la carpeta
# MAGIC también está `entrega_1.csv`. La tabla queda con **6** filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronce_copy_into
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 · Entrega 3 → `COPY INTO`
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_3.csv`** (carpeta `datos/` de este ejemplo) → **Upload**.
# MAGIC
# MAGIC Luego ejecuta la celda. **Qué esperar:** `num_affected_rows = 2` y la tabla con **8** filas. Bronce **acumula** todo
# MAGIC lo que llega, incluso las tres filas de Luis: decidir cuál vale es trabajo del `MERGE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronce_copy_into
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT entrega, archivo, count(*) AS filas FROM bronce_copy_into GROUP BY entrega, archivo ORDER BY entrega;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · `MERGE`, SCD1 y SCD2: preparar el escenario
# MAGIC Ahora haremos el recorrido **completo** de una carga incremental: por cada entrega, **subir el archivo →
# MAGIC `COPY INTO` a bronce → `MERGE` SCD1 → `MERGE` SCD2**.
# MAGIC
# MAGIC ### 3.1 · Vaciar el volumen otra vez
# MAGIC Borramos los 3 CSV para volver a subirlos uno por uno.
# MAGIC
# MAGIC **Qué esperar:** `Borrado:` de `entrega_1.csv`, `entrega_2.csv` y `entrega_3.csv`; `LIST` no devuelve filas.

# COMMAND ----------

carpeta = "/Volumes/workspace/demo_incremental/archivos/"
for archivo in dbutils.fs.ls(carpeta):
    if archivo.name.endswith(".csv"):
        dbutils.fs.rm(archivo.path)
        print("Borrado:", archivo.name)

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/workspace/demo_incremental/archivos/';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 · Las tablas del recorrido
# MAGIC | Tabla | Capa | Qué guarda |
# MAGIC |---|---|---|
# MAGIC | `bronce_clientes` | Bronce | **Todas** las filas de **todas** las entregas, tal como llegan (con `COPY INTO`) |
# MAGIC | `clientes_scd1` | Plata | **Un** cliente por id, con su valor más reciente |
# MAGIC | `clientes_scd2` | Plata | **Una fila por versión** del cliente: `fecha_inicio`, `fecha_fin` (`NULL` = vigente) y `es_actual` |
# MAGIC
# MAGIC > **¿Por qué una tabla de bronce nueva?** `COPY INTO` recuerda **en la tabla** qué archivos ya cargó.
# MAGIC > `bronce_copy_into` ya cargó `entrega_1.csv`, `entrega_2.csv` y `entrega_3.csv`: si volvemos a subir archivos con
# MAGIC > esos nombres, los **saltaría**. En una tabla nueva, `COPY INTO` empieza sin memoria.
# MAGIC
# MAGIC **Qué esperar:** `OK`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronce_clientes (entrega INT, id INT, nombre STRING, ciudad STRING, fecha_cambio DATE, archivo STRING);
# MAGIC CREATE TABLE clientes_scd1   (id INT, nombre STRING, ciudad STRING, fecha_cambio DATE);
# MAGIC CREATE TABLE clientes_scd2   (id INT, nombre STRING, ciudad STRING, fecha_inicio DATE, fecha_fin DATE, es_actual BOOLEAN);

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 · Los dos `MERGE` que aplicaremos en cada entrega
# MAGIC **`MERGE` SCD tipo 1 → `clientes_scd1`.** Compara la entrega con la tabla por la llave (`id`):
# MAGIC
# MAGIC | Cláusula | Cuándo | Qué hacemos |
# MAGIC |---|---|---|
# MAGIC | `WHEN MATCHED AND <cambió>` | El id ya existe y algún dato es distinto | `UPDATE SET *`: **sobrescribir** |
# MAGIC | `WHEN NOT MATCHED` | El id no existe | `INSERT *`: agregar |
# MAGIC | *(ninguna)* | Existe y nada cambió | No se toca |
# MAGIC
# MAGIC SCD tipo 1: *«Actualiza los registros directamente. No se conserva la historia de los registros actualizados»*.
# MAGIC
# MAGIC **`MERGE` SCD tipo 2 → `clientes_scd2`.** SCD tipo 2: *«Conserva la historia de los registros, ya sea en todas las
# MAGIC actualizaciones o solo en las de un conjunto de columnas indicado»*. Cuando un cliente cambia hay que hacer **dos**
# MAGIC cosas: **cerrar** su versión vigente (`UPDATE`) e **insertar** la nueva (`INSERT`). Un `MERGE` hace una sola acción
# MAGIC por fila de la fuente, así que la fuente lleva **dos copias** de los clientes que cambiaron:
# MAGIC
# MAGIC | Copia | `llave` | Qué le pasa |
# MAGIC |---|---|---|
# MAGIC | a) Todos los de la entrega | `id` | Si cambió, coincide con su versión vigente → **UPDATE** (la cierra). Si es nuevo → **INSERT** |
# MAGIC | b) Solo los que cambiaron | `NULL` | Una llave nula **nunca** coincide → **INSERT** de la versión nueva |
# MAGIC
# MAGIC Cada `MERGE` toma de bronce **solo la entrega de esa ronda** (`WHERE entrega = N`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Entrega 1: la carga inicial
# MAGIC ### 4.1 · Subir el archivo y cargar bronce con `COPY INTO`
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_1.csv`** (carpeta `datos/` de este ejemplo) → **Upload**. Trae a Ana (Bogotá), Luis (Cali) y Marta (Medellín).
# MAGIC
# MAGIC Luego ejecuta la celda. **Qué esperar:** `num_affected_rows = 3`.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronce_clientes
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 · `MERGE` SCD1
# MAGIC **Qué esperar:** `num_inserted_rows = 3`: los tres son nuevos.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd1 AS t
# MAGIC USING (SELECT id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 1) AS s   -- la entrega 1
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 · `MERGE` SCD2
# MAGIC **Qué esperar:** `num_inserted_rows = 3` (todos nuevos; la parte b de la fuente sale vacía).

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd2 AS t
# MAGIC USING (
# MAGIC   SELECT id AS llave, id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 1        -- a) todos
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS llave, s.id, s.nombre, s.ciudad, s.fecha_cambio                                     -- b) los que cambiaron
# MAGIC   FROM bronce_clientes AS s JOIN clientes_scd2 AS v ON v.id = s.id AND v.es_actual
# MAGIC   WHERE s.entrega = 1 AND (s.nombre <> v.nombre OR s.ciudad <> v.ciudad)
# MAGIC ) AS s
# MAGIC ON t.id = s.llave AND t.es_actual
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN
# MAGIC   UPDATE SET es_actual = false, fecha_fin = s.fecha_cambio                      -- cierra la versión vigente
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, nombre, ciudad, fecha_inicio, fecha_fin, es_actual)
# MAGIC   VALUES (s.id, s.nombre, s.ciudad, s.fecha_cambio, NULL, true);                -- nueva versión vigente

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 · Resultado después de la entrega 1
# MAGIC **Qué esperar:** las dos tablas iguales: 3 clientes vigentes.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'SCD1' AS tabla, id, nombre, ciudad, fecha_cambio AS desde, CAST(NULL AS DATE) AS hasta, true AS es_actual
# MAGIC FROM clientes_scd1
# MAGIC UNION ALL
# MAGIC SELECT 'SCD2', id, nombre, ciudad, fecha_inicio, fecha_fin, es_actual FROM clientes_scd2
# MAGIC ORDER BY tabla, id, desde;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Entrega 2: un cambio, un nuevo y un reenvío
# MAGIC ### 5.1 · Subir el archivo y cargar bronce con `COPY INTO`
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_2.csv`** (carpeta `datos/` de este ejemplo) → **Upload**. Trae a Luis en **Barranquilla**, a Pedro (nuevo) y a Ana **sin cambios**.
# MAGIC
# MAGIC Luego ejecuta la celda. **Qué esperar:** `num_affected_rows = 3`: solo carga `entrega_2.csv`, aunque en la carpeta sigue
# MAGIC `entrega_1.csv`. Bronce queda con **6** filas.

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronce_clientes
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 · `MERGE` SCD1
# MAGIC **Qué esperar:** `num_updated_rows = 1` (Luis → Barranquilla) y `num_inserted_rows = 1` (Pedro). Ana no se toca:
# MAGIC llegó igual.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd1 AS t
# MAGIC USING (SELECT id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 2) AS s   -- la entrega 2
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 · `MERGE` SCD2
# MAGIC **Qué esperar:** `num_updated_rows = 1` (se **cierra** Luis-Cali) y `num_inserted_rows = 2` (Luis-Barranquilla y
# MAGIC Pedro). Ana no se toca.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd2 AS t
# MAGIC USING (
# MAGIC   SELECT id AS llave, id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 2        -- a) todos
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS llave, s.id, s.nombre, s.ciudad, s.fecha_cambio                                     -- b) los que cambiaron
# MAGIC   FROM bronce_clientes AS s JOIN clientes_scd2 AS v ON v.id = s.id AND v.es_actual
# MAGIC   WHERE s.entrega = 2 AND (s.nombre <> v.nombre OR s.ciudad <> v.ciudad)
# MAGIC ) AS s
# MAGIC ON t.id = s.llave AND t.es_actual
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN
# MAGIC   UPDATE SET es_actual = false, fecha_fin = s.fecha_cambio                      -- cierra la versión vigente
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, nombre, ciudad, fecha_inicio, fecha_fin, es_actual)
# MAGIC   VALUES (s.id, s.nombre, s.ciudad, s.fecha_cambio, NULL, true);                -- nueva versión vigente

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 · Resultado después de la entrega 2
# MAGIC **Qué esperar:**
# MAGIC - SCD1: **4** filas, una por cliente; Luis ya está en Barranquilla y **no queda rastro** de Cali.
# MAGIC - SCD2: **5** filas; Luis tiene dos versiones: Cali (cerrada, `hasta = 2026-11-03`) y Barranquilla (vigente).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'SCD1' AS tabla, id, nombre, ciudad, fecha_cambio AS desde, CAST(NULL AS DATE) AS hasta, true AS es_actual
# MAGIC FROM clientes_scd1
# MAGIC UNION ALL
# MAGIC SELECT 'SCD2', id, nombre, ciudad, fecha_inicio, fecha_fin, es_actual FROM clientes_scd2
# MAGIC ORDER BY tabla, id, desde;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 · Repetir el `MERGE` no cambia nada
# MAGIC Volvemos a aplicar la entrega 2 a SCD1. **Qué esperar:** `num_affected_rows = 0`: Luis ya está en Barranquilla y
# MAGIC Pedro ya existe. El `MERGE` también es idempotente.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd1 AS t
# MAGIC USING (SELECT id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 2) AS s   -- la entrega 2
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Entrega 3: Luis se muda otra vez
# MAGIC ### 6.1 · Subir el archivo y cargar bronce con `COPY INTO`
# MAGIC 👉 **Paso manual:** **Catalog** → `workspace` → `demo_incremental` → **Volumes** → `archivos` →
# MAGIC **Upload to this volume** → arrastra **`entrega_3.csv`** (carpeta `datos/` de este ejemplo) → **Upload**. Trae a Luis en **Bogotá** y a Sofía (nueva).
# MAGIC
# MAGIC Luego ejecuta la celda. **Qué esperar:** `num_affected_rows = 2` y bronce con **8** filas (3 + 3 + 2).

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO bronce_clientes
# MAGIC FROM (
# MAGIC   SELECT CAST(entrega AS INT) AS entrega, CAST(id AS INT) AS id, nombre, ciudad,
# MAGIC          CAST(fecha_cambio AS DATE) AS fecha_cambio, _metadata.file_name AS archivo
# MAGIC   FROM '/Volumes/workspace/demo_incremental/archivos/'
# MAGIC )
# MAGIC FILEFORMAT = CSV
# MAGIC PATTERN = 'entrega_*.csv'
# MAGIC FORMAT_OPTIONS ('header' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 · `MERGE` SCD1
# MAGIC **Qué esperar:** `num_updated_rows = 1` (Luis → Bogotá) y `num_inserted_rows = 1` (Sofía).

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd1 AS t
# MAGIC USING (SELECT id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 3) AS s   -- la entrega 3
# MAGIC ON t.id = s.id
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 · `MERGE` SCD2
# MAGIC **Qué esperar:** `num_updated_rows = 1` (se cierra Luis-Barranquilla) y `num_inserted_rows = 2` (Luis-Bogotá y Sofía).

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO clientes_scd2 AS t
# MAGIC USING (
# MAGIC   SELECT id AS llave, id, nombre, ciudad, fecha_cambio FROM bronce_clientes WHERE entrega = 3        -- a) todos
# MAGIC   UNION ALL
# MAGIC   SELECT NULL AS llave, s.id, s.nombre, s.ciudad, s.fecha_cambio                                     -- b) los que cambiaron
# MAGIC   FROM bronce_clientes AS s JOIN clientes_scd2 AS v ON v.id = s.id AND v.es_actual
# MAGIC   WHERE s.entrega = 3 AND (s.nombre <> v.nombre OR s.ciudad <> v.ciudad)
# MAGIC ) AS s
# MAGIC ON t.id = s.llave AND t.es_actual
# MAGIC WHEN MATCHED AND (t.nombre <> s.nombre OR t.ciudad <> s.ciudad) THEN
# MAGIC   UPDATE SET es_actual = false, fecha_fin = s.fecha_cambio                      -- cierra la versión vigente
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (id, nombre, ciudad, fecha_inicio, fecha_fin, es_actual)
# MAGIC   VALUES (s.id, s.nombre, s.ciudad, s.fecha_cambio, NULL, true);                -- nueva versión vigente

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 · Resultado después de la entrega 3
# MAGIC **Qué esperar:**
# MAGIC - SCD1: **5** clientes; Luis en Bogotá. Ya **no sabemos** que vivió en Cali ni en Barranquilla.
# MAGIC - SCD2: **7** filas; Luis tiene **3 versiones** y solo la de Bogotá tiene `es_actual = true`.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'SCD1' AS tabla, id, nombre, ciudad, fecha_cambio AS desde, CAST(NULL AS DATE) AS hasta, true AS es_actual
# MAGIC FROM clientes_scd1
# MAGIC UNION ALL
# MAGIC SELECT 'SCD2', id, nombre, ciudad, fecha_inicio, fecha_fin, es_actual FROM clientes_scd2
# MAGIC ORDER BY tabla, id, desde;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Lo que solo permite SCD2 y comparación final
# MAGIC ### 7.1 · Viajar al pasado: ¿dónde vivía cada cliente el 3 de noviembre?
# MAGIC Una versión estaba vigente en la fecha `X` si `fecha_inicio <= X` y (`fecha_fin` es `NULL` o `fecha_fin > X`).
# MAGIC
# MAGIC **Qué esperar:** 4 clientes; Luis en **Barranquilla** y Sofía todavía no existe. Con SCD1 esta pregunta no tiene
# MAGIC respuesta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT id, nombre, ciudad, fecha_inicio, fecha_fin
# MAGIC FROM clientes_scd2
# MAGIC WHERE fecha_inicio <= DATE'2026-11-03'
# MAGIC   AND (fecha_fin IS NULL OR fecha_fin > DATE'2026-11-03')
# MAGIC ORDER BY id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 · Comparación final
# MAGIC **Qué esperar:** las dos tablas tienen los mismos **5 clientes vigentes**; SCD2 guarda además **2 versiones
# MAGIC históricas** de Luis.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'SCD1' AS tipo, count(*) AS filas, count(*) AS vigentes, 0 AS historicas FROM clientes_scd1
# MAGIC UNION ALL
# MAGIC SELECT 'SCD2', count(*), count_if(es_actual), count_if(NOT es_actual) FROM clientes_scd2;

# COMMAND ----------

# MAGIC %md
# MAGIC | | `INSERT INTO` | `COPY INTO` | `MERGE` SCD1 | `MERGE` SCD2 |
# MAGIC |---|---|---|---|---|
# MAGIC | ¿Para qué? | Agregar filas | Cargar **archivos nuevos** a bronce | Aplicar cambios por **llave** | Aplicar cambios por **llave** |
# MAGIC | ¿Recuerda lo ya cargado? | No: duplica | Sí (en la tabla) | Sí: compara con el destino | Sí: compara con la versión vigente |
# MAGIC | ¿Qué pasa con el dato viejo? | Queda repetido | — | Se **sobrescribe** | Se **cierra** y se guarda |
# MAGIC | Se usa en… | — | Bronce | Plata | Plata |
# MAGIC
# MAGIC ### Limpieza (opcional)
# MAGIC Para borrar todo lo de este ejemplo, ejecuta: `DROP SCHEMA workspace.demo_incremental CASCADE;`