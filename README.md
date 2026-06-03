# Streaming Analytics Platform - Databricks Lakehouse Architecture

Este repositorio contiene el proyecto final para mi certificación como **Data Engineer**. En él, diseño e implemento un pipeline de datos robusto de punta a punta (End-to-End) para procesar a gran escala el ecosistema de datos de una plataforma de Streaming de Video, utilizando **Apache Spark**, **Delta Lake** y **Unity Catalog** sobre un entorno **Serverless**.

## 🛠️ Tecnologías y Herramientas Utilizadas
* **Plataforma:** Databricks Community Edition (Modern Serverless Compute)
* **Gobierno de Datos:** Unity Catalog (Catalogs, Schemas & Volumes)
* **Motor de Procesamiento:** Apache Spark / PySpark 3.x
* **Formato de Almacenamiento:** Delta Lake (Tablas transaccionales con soporte ACID)
* **Análisis:** Spark SQL nativo

---

## 🏗️ Arquitectura del Pipeline (Modelo Medallón)

El proyecto ingesta e interconecta un modelo de datos complejo compuesto por **16 datasets normalizados**, los cuales se segregan lógicamente en tres capas funcionales:

### 1️⃣ Capa Bronze (Raw Ingestion)
* **Origen:** Archivos CSV crudos almacenados de forma segura dentro de un *Volumen* de Unity Catalog.
* **Procesamiento:** Implementación de un proceso automatizado mediante un bucle iterativo en PySpark que lee paralelamente el lote completo de archivos, infiere esquemas dinámicamente y añade columnas de auditoría (`fecha_ingesta`) sin alterar la granularidad original.
* **Persistencia:** Guardado inicial en formato Delta Open-Source.

### 2️⃣ Capa Silver (Data Quality & Cleaning)
* **Tratamiento de Dimensiones (`silver_streaming_users`):** Remoción de duplicados analíticos mediante llaves primarias (`user_id`) y estandarización de campos nulos geográficos utilizando funciones distributivas (`coalesce`).
* **Tratamiento de Hechos (`silver_streaming_watch_history`):** Filtrado de registros huérfanos sin identificadores de transacción (`watch_id`) y tipado estricto de estructuras temporales.

### 3️⃣ Capa Gold (Business Insights)
* **Desafío Relacional Resuelto:** La tabla transaccional de reproducciones registra el comportamiento a nivel de perfil de pantalla (`profile_id`), no de usuario. Diseñé un **Join en cadena** uniendo tres tablas en Spark (`Historial ➡️ Perfiles ➡️ Usuarios`) para consolidar la trazabilidad del negocio.
* **Resolución de Ambigüedades:** Manejo explícito de referencias de columnas colisionadas (`df_users.country`) para evitar punteros ambiguos en la agregación.
* **Resultado:** Un Data Mart de alto valor que computa el total de reproducciones por país de residencia.

---

## ⚡ Performance Engineering & Optimización Delta

Para certificar las directrices del curso y evitar errores críticos de rendimiento en Big Data (como el problema de los archivos pequeños), apliqué ingeniería de optimización avanzada:

* **Z-ORDERING:** Reorganización física de los archivos de la tabla de hechos en base a la columna de mayor densidad de Joins (`profile_id`). Esto reduce drásticamente el escaneo de archivos físicos (*file skipping*) acelerando las consultas SQL futuras.
* **VACUUM Serverless:** Ejecución de comandos de purga de historial Delta respetando las directrices de resguardo analítico y políticas de retención estrictas impuestas por el gobierno Serverless de Databricks, garantizando transacciones ACID limpias.

---

## 📂 Estructura del Proyecto en el Workspace
* `01_ingestion_bronze.py`: Cuaderno PySpark enfocado en la carga masiva y automatizada al Data Lakehouse.
* `02_transformacion_silver.py`: Cuaderno enfocado en la aplicación de reglas de calidad de datos, limpieza y desduplicación.
* `03_analisis_gold_optimizacion.sql`: Cuaderno híbrido (PySpark/SQL) para la generación de métricas de negocio y optimización del motor de almacenamiento.
