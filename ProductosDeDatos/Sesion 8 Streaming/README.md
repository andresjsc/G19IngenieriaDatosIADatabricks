# Pipeline de Ingesta en Tiempo Real (Structured Streaming + Azure Event Hubs)

Laboratorio práctico de **Ingeniería de Datos e IA con Databricks** (Grupo G19 — Instructor: **Andrés Forero**).

Este módulo implementa la ingesta desacoplada en tiempo real desde **Azure Event Hubs** (vía protocolo **Apache Kafka**) hacia una arquitectura Medallón gobernada en **Unity Catalog** (`iot_sensores`), optimizada para cómputo *Serverless* mediante micro-lotes bajo demanda (`.trigger(availableNow=True)`).

---

## Arquitectura y Principios de Diseño

* **Gobierno y Seguridad de Credenciales:** Gestión centralizada de secretos (`stringconexioneventhub` y `topicoeventhub`) dentro del esquema `iot_sensores.control` en Unity Catalog, consumidos dinámicamente con `dbutils.secrets.get` y parametrizados vía `dbutils.widgets` sin exposición de credenciales en código.
* **Ingesta Streaming Desacoplada (`1.Nb_Bronce_RT_AvailableNow`):**
  * Conexión segura mediante `SASL_SSL` (`PLAIN`) contra el endpoint de Event Hubs (`:9093`).
  * Persistencia inmutable de eventos crudos (`key`, `value` en JSON, `topic`, `partition`, `offset`, `timestamp`, `headers` y `_fecha_ingesta`) en la tabla Delta `iot_sensores.bronce.mensajes_iot`.
* **Control Incremental e Idempotencia:** Uso de `checkpointLocation` persistente en Volúmenes de Unity Catalog (`/Volumes/iot_sensores/bronce/checkpoints/mensajes_iot`) para garantizar lectura *exactly-once* de offsets sin duplicar mensajes entre ejecuciones.
* **Optimización Serverless:** Ejecución con `.trigger(availableNow=True)` para procesar todos los eventos pendientes en micro-batches y liberar el cómputo automáticamente al finalizar.

---

## 📂 Estructura del Módulo

```text
ProductosDeDatos/Sesion 8 Streaming/
├── README.md                                  # Documentación técnica del pipeline de streaming
└── resource/
    └── notebook/
        ├── 0.Configuracion (1).py             # Setup inicial de catálogo, esquemas, volúmenes y secretos en Unity Catalog
        ├── 1.Nb_Bronce_RT_AvailableNow.py     # Ingesta Structured Streaming desde Event Hubs hacia Capa Bronce
        └── 99_Reiniciar_Lab.py                # Utilidad de limpieza y reinicio controlado del entorno de pruebas
