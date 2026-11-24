# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 1 · Notebook 0 — Configuración
# MAGIC **Curso:** Ingeniería de Datos e IA con Databricks · **Clase 08:** Real time streaming · **Producto de datos:** Telemetría
# MAGIC
# MAGIC En las clases anteriores los datos llegaban en **archivos**: uno completo (Clase 05) o uno por día (Clase 07).
# MAGIC Ahora llegan como **eventos**: un sensor envía una lectura de temperatura y humedad **cada 2 segundos**, sin parar.
# MAGIC Ya no hay archivos que subir: los mensajes viajan por Azure y Databricks los lee con **Spark Structured Streaming**.
# MAGIC
# MAGIC ```
# MAGIC  AZURE                                                     DATABRICKS (catálogo iot_sensores)
# MAGIC  Simulador        IoT Hub        ruta         Event Hubs      bronce             plata                    oro
# MAGIC  Raspberry Pi ──► (device) ──► (routing) ──► telemetria- ──► mensajes    ──►    lecturas con      ──►    métrica por
# MAGIC  cada 2 s                                     sensores        tal como           columnas claras         minuto y
# MAGIC                                               (Kafka)         llegan             (organizadas)           dispositivo
# MAGIC ```
# MAGIC
# MAGIC | Catálogo | Schema | Objetos |
# MAGIC |---|---|---|
# MAGIC | `iot_sensores` | `control` | **Secretos** para conectarse al event hub: la *connection string* y el nombre del topic |
# MAGIC | `iot_sensores` | `bronce` | `mensajes_iot`: cada mensaje **tal como llega** · volumen `checkpoints` |
# MAGIC | `iot_sensores` | `plata` | `lecturas_sensores`: las lecturas **organizadas** (columnas, nombres claros, limpieza) · volumen `checkpoints` |
# MAGIC | `iot_sensores` | `oro` | `metricas_minuto_dispositivo`: temperatura y humedad **por minuto** |
# MAGIC
# MAGIC **¿Para qué los volúmenes `checkpoints`?** Un stream guarda ahí **hasta dónde leyó** (el *offset* de cada partición).
# MAGIC Gracias a eso, cada ejecución continúa donde quedó la anterior: no repite ni pierde mensajes.
# MAGIC
# MAGIC **Antes de empezar:** pulsa **Connect** y elige **Serverless**.
# MAGIC
# MAGIC Fuentes oficiales: [CREATE CATALOG](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-catalog) ·
# MAGIC [CREATE SCHEMA](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-schema) ·
# MAGIC [CREATE VOLUME](https://learn.microsoft.com/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-volume) ·
# MAGIC [Structured Streaming checkpoints](https://learn.microsoft.com/azure/databricks/structured-streaming/checkpoints) ·
# MAGIC [Secrets in Unity Catalog](https://learn.microsoft.com/azure/databricks/security/secrets/unity-catalog-secrets).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Crear el catálogo, los schemas y los volúmenes
# MAGIC Todo usa `IF NOT EXISTS`: si vuelves a ejecutar la celda no pasa nada.
# MAGIC
# MAGIC El schema **`control`** no guarda tablas: guarda los **secretos** del producto de datos (sección 2).
# MAGIC
# MAGIC **Qué esperar:** `OK`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS iot_sensores COMMENT 'Producto de datos Telemetría: sensores IoT en tiempo real';
# MAGIC CREATE SCHEMA  IF NOT EXISTS iot_sensores.control COMMENT 'Control: secretos para conectarse al event hub';
# MAGIC CREATE SCHEMA  IF NOT EXISTS iot_sensores.bronce  COMMENT 'Bronce: mensajes del Event Hub tal como llegan';
# MAGIC CREATE SCHEMA  IF NOT EXISTS iot_sensores.plata   COMMENT 'Plata: lecturas organizadas, con columnas claras';
# MAGIC CREATE SCHEMA  IF NOT EXISTS iot_sensores.oro     COMMENT 'Oro: métricas listas para consumir';
# MAGIC
# MAGIC CREATE VOLUME IF NOT EXISTS iot_sensores.bronce.checkpoints COMMENT 'Checkpoints del stream Event Hubs → bronce';
# MAGIC CREATE VOLUME IF NOT EXISTS iot_sensores.plata.checkpoints  COMMENT 'Checkpoints del stream bronce → plata';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Guardar la conexión al event hub como secretos
# MAGIC La *connection string* del event hub es una **contraseña**: no se escribe en el código ni se deja en un widget,
# MAGIC porque terminaría en el repositorio. Se guarda como **secreto de Unity Catalog** dentro del schema
# MAGIC `iot_sensores.control`, y el notebook de bronce la lee con `dbutils.secrets.get(...)`, que además la
# MAGIC **oculta** (`[REDACTED]`) si alguien intenta imprimirla.
# MAGIC
# MAGIC | Secreto | Valor | De dónde sale |
# MAGIC |---|---|---|
# MAGIC | `stringconexioneventhub` | `Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=databricks-lectura;SharedAccessKey=…;EntityPath=telemetria-sensores` | README, paso 2.3: **Connection string–primary key** de la política `databricks-lectura` |
# MAGIC | `topicoeventhub` | `telemetria-sensores` | El nombre del event hub (el *topic* de Kafka) |
# MAGIC
# MAGIC Se crean **desde la interfaz** (2.1) y se comprueban con código (2.2).
# MAGIC
# MAGIC > Requisitos: workspace con Unity Catalog y cómputo **Serverless** (versión de entorno 4 o superior). Para crear
# MAGIC > secretos necesitas ser dueño del schema o tener `CREATE SECRET` en él; para leerlos, `READ SECRET`.
# MAGIC
# MAGIC ### 2.1 · Crear los secretos desde la interfaz
# MAGIC 1. En la barra lateral, pulsa **Catalog**.
# MAGIC 2. Abre el catálogo **`iot_sensores`** → schema **`control`** (si no aparece, ejecuta antes la sección 1).
# MAGIC 3. Pulsa **Create → Secret**.
# MAGIC 4. **Name:** `stringconexioneventhub` · **Value:** pega la *connection string* de `databricks-lectura`.
# MAGIC    *(Opcional)* **Comment** y **Expiration date**. **No** pongas el valor ni pistas de él en el comentario.
# MAGIC 5. Pulsa **Create**.
# MAGIC 6. Repite con **Name:** `topicoeventhub` · **Value:** `telemetria-sensores`.
# MAGIC
# MAGIC Los secretos aparecen en el schema `control`, pero su valor **no** se muestra.
# MAGIC Guía: [Create a secret in Catalog Explorer](https://learn.microsoft.com/azure/databricks/security/secrets/unity-catalog-secrets).

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 · Comprobar los secretos
# MAGIC `dbutils.secrets.list` muestra los secretos del schema; `dbutils.secrets.get` lee uno. Al imprimirlo, Databricks
# MAGIC muestra `[REDACTED]`: el valor se puede **usar** en el código, pero no **ver** en la salida.
# MAGIC
# MAGIC **Qué esperar:** los dos secretos en la lista, `[REDACTED]` en lugar del valor y `True`.

# COMMAND ----------

for secreto in dbutils.secrets.list(catalog="iot_sensores", schema="control"):
    print(secreto.key)

cadena = dbutils.secrets.get(catalog="iot_sensores", schema="control", key="stringconexioneventhub")
print("Valor leído:", cadena, "| Empieza por Endpoint=sb://:", cadena.startswith("Endpoint=sb://"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Comprobar los volúmenes
# MAGIC **Qué esperar:** los dos volúmenes `checkpoints` (vacíos por ahora).

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN iot_sensores.bronce;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW VOLUMES IN iot_sensores.plata;

# COMMAND ----------

# MAGIC %md
# MAGIC **Siguiente paso:** con el simulador **enviando mensajes**, abre `1.Nb_Bronce_RT_AvailableNow`.