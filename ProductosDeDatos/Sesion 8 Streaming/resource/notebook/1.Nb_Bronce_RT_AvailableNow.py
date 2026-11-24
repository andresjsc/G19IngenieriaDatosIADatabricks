# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Lab 1 · Notebook 1 — Bronce: leer el Event Hub con Structured Streaming
# MAGIC **Lee:** event hub `telemetria-sensores` (protocolo Kafka) · **Escribe:** `iot_sensores.bronce.mensajes_iot`
# MAGIC
# MAGIC El simulador envía cada lectura al **IoT Hub**; una **ruta** de IoT Hub la reenvía al **event hub**
# MAGIC `telemetria-sensores`. Event Hubs (tier Standard) habla el protocolo **Apache Kafka**, así que Databricks lo lee con
# MAGIC el conector **Kafka** que ya trae incorporado: no hay que instalar librerías.
# MAGIC
# MAGIC | Concepto de Kafka | En Event Hubs |
# MAGIC |---|---|
# MAGIC | Clúster (`bootstrap.servers`) | Namespace: `<namespace>.servicebus.windows.net:9093` |
# MAGIC | Topic | El event hub: `telemetria-sensores` |
# MAGIC | Partición / Offset | Partición / Offset |
# MAGIC
# MAGIC **¿Cómo es el «tiempo real» en serverless?** En cómputo serverless el stream se ejecuta con el trigger
# MAGIC **`availableNow`**: lee **todo lo que llegó desde la última ejecución**, lo escribe y se detiene. El *checkpoint*
# MAGIC recuerda hasta qué offset leyó, así que la siguiente ejecución sigue desde ahí. Mientras el simulador envía, cada
# MAGIC ejecución trae los mensajes nuevos (unos **30 por minuto** por dispositivo).
# MAGIC
# MAGIC **Bronce guarda el mensaje tal como llega:** el cuerpo (`value`) es el JSON del simulador sin tocar, junto con los
# MAGIC metadatos de Kafka (`topic`, `partition`, `offset`, `timestamp`…).
# MAGIC
# MAGIC **Prerrequisito:** `0.Nb_Configuracion` (con los **secretos** del event hub en `iot_sensores.control`) y el simulador enviando.
# MAGIC
# MAGIC Fuentes oficiales: [Stream processing with Apache Kafka and Azure Databricks](https://learn.microsoft.com/azure/databricks/connect/streaming/kafka) ·
# MAGIC [Kafka authentication (SASL/PLAIN)](https://learn.microsoft.com/azure/databricks/connect/streaming/kafka/authentication) ·
# MAGIC [Event Hubs for Apache Kafka](https://learn.microsoft.com/azure/event-hubs/azure-event-hubs-apache-kafka-overview) ·
# MAGIC [Structured Streaming + Kafka (Apache Spark)](https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html) ·
# MAGIC [Secrets in Unity Catalog](https://learn.microsoft.com/azure/databricks/security/secrets/unity-catalog-secrets) ·
# MAGIC [Run jobs continuously](https://learn.microsoft.com/azure/databricks/jobs/continuous) ·
# MAGIC [Serverless compute limitations](https://learn.microsoft.com/azure/databricks/compute/serverless/limitations).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Conexión con el event hub
# MAGIC La conexión **no** se escribe aquí: está guardada como **secretos de Unity Catalog** en `iot_sensores.control`
# MAGIC (notebook 0, sección 2). Los widgets solo traen el **nombre** de cada secreto:
# MAGIC
# MAGIC | Widget | Valor por defecto | Secreto que lee |
# MAGIC |---|---|---|
# MAGIC | `secreto_conexion` | `stringconexioneventhub` | La *connection string* de la política `databricks-lectura` |
# MAGIC | `secreto_topico` | `topicoeventhub` | El nombre del event hub (el *topic*) |
# MAGIC
# MAGIC En un **Job**, se pasan como **parámetros del job** con esos mismos nombres. `dbutils.secrets.get(catalog=…,
# MAGIC schema=…, key=…)` devuelve el valor; si intentas imprimirlo, Databricks muestra `[REDACTED]`.
# MAGIC
# MAGIC Con la cadena armamos las opciones de Kafka:
# MAGIC
# MAGIC | Opción | Valor | Por qué |
# MAGIC |---|---|---|
# MAGIC | `kafka.bootstrap.servers` | `<namespace>.servicebus.windows.net:9093` | 9093 es el puerto Kafka de Event Hubs |
# MAGIC | `kafka.security.protocol` · `kafka.sasl.mechanism` | `SASL_SSL` · `PLAIN` | Autenticación con usuario y contraseña sobre TLS |
# MAGIC | `kafka.sasl.jaas.config` | usuario `$ConnectionString`, contraseña = la cadena | Así lo pide Event Hubs. En Databricks la clase va con el prefijo `kafkashaded.` |
# MAGIC | `subscribe` | `telemetria-sensores` | El topic que se lee |
# MAGIC | `startingOffsets` | `earliest` | La **primera** vez lee desde el mensaje más antiguo que conserva el event hub. Después manda el checkpoint |
# MAGIC | `includeHeaders` | `true` | Trae también los *headers* del mensaje |
# MAGIC
# MAGIC **Qué esperar:** `Servidor Kafka: <tu-namespace>.servicebus.windows.net:9093 | Topic: [REDACTED]` (el topic
# MAGIC también viene de un secreto, por eso Databricks lo oculta). Si sale `Secret does not exist` o `PERMISSION_DENIED`, revisa el nombre del secreto y que exista en
# MAGIC `iot_sensores.control` (notebook 0, sección 2).

# COMMAND ----------

import re
from pyspark.sql import functions as F

dbutils.widgets.text("secreto_conexion", "stringconexioneventhub", "Secreto: connection string")
dbutils.widgets.text("secreto_topico",   "topicoeventhub",         "Secreto: topic (event hub)")

catalogo_secretos, schema_secretos = "iot_sensores", "control"
cadena_conexion = dbutils.secrets.get(catalog=catalogo_secretos, schema=schema_secretos,
                                      key=dbutils.widgets.get("secreto_conexion").strip()).strip()
event_hub       = dbutils.secrets.get(catalog=catalogo_secretos, schema=schema_secretos,
                                      key=dbutils.widgets.get("secreto_topico").strip()).strip()

servidor = re.search(r"sb://([^/;]+)", cadena_conexion)
if servidor is None:
    raise Exception("El secreto de la conexión no es una connection string de Event Hubs (debe empezar por Endpoint=sb://).")
servidor_kafka = f"{servidor.group(1)}:9093"

opciones_kafka = {
    "kafka.bootstrap.servers": servidor_kafka,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism":    "PLAIN",
    "kafka.sasl.jaas.config":  ("kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
                                f'username="$ConnectionString" password="{cadena_conexion}";'),
    "subscribe":               event_hub,
    "startingOffsets":         "earliest",
    "includeHeaders":          "true",
}

tabla_bronce      = "iot_sensores.bronce.mensajes_iot"
checkpoint_bronce = "/Volumes/iot_sensores/bronce/checkpoints/mensajes_iot"

print("Servidor Kafka:", servidor_kafka, "| Topic:", event_hub)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El stream: Event Hub → bronce
# MAGIC | Parte | Qué hace |
# MAGIC |---|---|
# MAGIC | `spark.readStream.format("kafka")` | Lee el topic como una **tabla que crece**: cada mensaje nuevo es una fila nueva |
# MAGIC | `.select(...)` | `key` y `value` a texto + `_fecha_ingesta`. **No** se interpreta el JSON: eso es trabajo de plata |
# MAGIC | `.writeStream.toTable(...)` | Agrega (`append`) las filas a la tabla Delta de bronce; si no existe, la crea |
# MAGIC | `checkpointLocation` | Carpeta del volumen donde el stream guarda los offsets ya procesados |
# MAGIC | `trigger(availableNow=True)` | Procesa todo lo pendiente y **se detiene** (el trigger que admite serverless) |
# MAGIC
# MAGIC `awaitTermination()` espera a que el stream termine. Con `availableNow` **sí termina**: cuando procesó lo
# MAGIC pendiente se detiene y el notebook acaba. Si el stream falla, el error aparece en esta celda.
# MAGIC
# MAGIC > **Dos «continuos» distintos.** Para que esto corra sin parar en serverless, el notebook se pone en un **Job de
# MAGIC > Lakeflow con trigger _Continuous_**, que lo vuelve a lanzar apenas termina; dentro se deja `availableNow`.
# MAGIC > Eso **no** es el trigger de Spark `.trigger(continuous="1 second")`, que ninguna edición de Databricks soporta.
# MAGIC
# MAGIC **Qué esperar:** la primera vez, todos los mensajes que había en el event hub.
# MAGIC 👉 **Prueba:** espera 1 minuto y ejecuta la celda **otra vez**: solo carga los mensajes **nuevos** (unos 30 por
# MAGIC dispositivo). Si detienes el simulador y la ejecutas de nuevo, carga **0**.

# COMMAND ----------

df_stream = (spark.readStream
             .format("kafka")
             .options(**opciones_kafka)
             .load()
             .select(F.col("key").cast("string").alias("key"),
                     F.col("value").cast("string").alias("value"),
                     "topic", "partition", "offset", "timestamp", "timestampType", "headers",
                     F.current_timestamp().alias("_fecha_ingesta")))

consulta = (df_stream.writeStream
            .option("checkpointLocation", checkpoint_bronce)
            .trigger(availableNow=True)
            .toTable(tabla_bronce))

consulta.awaitTermination()
