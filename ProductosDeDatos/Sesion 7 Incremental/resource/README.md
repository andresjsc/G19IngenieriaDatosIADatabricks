# Pipeline Incremental Medallón en Databricks (Unity Catalog + Delta Lake)

Repositorio de prácticas de **Ingeniería de Datos e IA con Databricks** (Grupo G19 — Instructor: **Andrés Forero**).

Este proyecto implementa un pipeline incremental desacoplado bajo **Arquitectura Medallón (Bronce, Plata y Oro)** en **Unity Catalog**, separando la lógica de negocio en scripts `.sql` puros y orquestando la ejecución mediante un **Databricks Job** con disparador automático por llegada de archivos (*File Arrival Trigger*).

---

##  Arquitectura y Principios de Diseño

* **Separación de responsabilidades:** Scripts SQL desacoplados en archivos planos `.sql` e inyección dinámica de parámetros (`{catalogo}`, `{esquema}`) mediante una utilidad reutilizable en Python (`sql_runner.py`).
* **Idempotencia:** Re-ejecuciones seguras sin duplicidad de registros en ninguna capa.
* **Capa Bronce (`bronce_clientes`):** Ingesta incremental mediante `COPY INTO` preservando la evidencia cruda e inmutable con metadatos de auditoría (`archivo` y `createat`).
* **Capa Plata (`clientes_scd1` y `clientes_scd2`):**
  * **SCD Tipo 1:** Actualización de fotografía vigente mediante `MERGE` condicional (solo actualiza si hubo cambio real de atributos).
  * **SCD Tipo 2:** Trazabilidad histórica completa mediante `MERGE` + `INSERT` con control de vigencia (`fecha_inicio`, `fecha_fin`, `es_actual`).
* **Capa Oro (`oro_clientes_por_ciudad`):** Recálculo idempotente de métricas agregadas (`INSERT OVERWRITE`) listas para consumo analítico.
* **Orquestación Automatizada:** DAG de 3 tareas (`01_bronce_clientes` → `02_plata_clientes` → `03_oro_clientes`) ejecutado sobre cómputo *Serverless* y activado automáticamente al depositar archivos CSV en el Volumen de Unity Catalog.

---

## 📂 Estructura del Proyecto

```text
ProductosDeDatos/Sesion 7 Incremental/resource/
├── common/
│   └── sql_runner.py                  # Motor reutilizable para leer y ejecutar scripts .sql parametrizados
├── notebook/
│   ├── 00_setup_infraestructura       # Creación única de catálogo, esquema, volumen y tablas Delta
│   ├── 01_nb_bronce_clientes          # Orquestador Capa Bronce (COPY INTO)
│   ├── 02_nb_plata_clientes           # Orquestador Capa Plata (SCD Tipo 1 y SCD Tipo 2)
│   ├── 03_nb_oro_clientes             # Orquestador Capa Oro (Agregación por ciudad)
│   └── 04_nb_auditoria_medallon       # Cuaderno de validación y auditoría end-to-end
└── sql/
    ├── 00_setup_ddl.sql               # DDL de infraestructura y tablas Delta Lake
    ├── 01_bronce_copy_into.sql        # Sentencia COPY INTO incremental hacia Bronce
    ├── 02_plata_merge_scd1.sql        # Lógica MERGE idempotente para SCD Tipo 1
    ├── 02_plata_merge_scd2.sql        # Lógica MERGE + INSERT histórico para SCD Tipo 2
    └── 03_oro_clientes_por_ciudad.sql # Cálculo agregado de clientes vigentes por ciudad
