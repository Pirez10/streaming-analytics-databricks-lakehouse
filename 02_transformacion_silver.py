# Databricks notebook source
spark.table("plataforma_streaming.default.bronze_streaming_users").printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, coalesce, lit

print("--- Iniciando Procesamiento Capa Silver ---")

# =====================================================================
# 1. LIMPIEZA DE LA DIMENSIÓN DE USUARIOS
# =====================================================================
df_users_bronze = spark.table("plataforma_streaming.default.bronze_streaming_users")

df_users_silver = df_users_bronze \
    .dropDuplicates(["user_id"]) \
    .withColumn("country", coalesce(col("country"), lit("Desconocido")))

df_users_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("plataforma_streaming.default.silver_streaming_users")

print("-> Tabla 'silver_streaming_users' creada con éxito.")


# =====================================================================
# 2. LIMPIEZA DE LA TABLA DE HECHOS (HISTORIAL DE REPRODUCCIÓN)
# =====================================================================
df_history_bronze = spark.table("plataforma_streaming.default.bronze_streaming_watch_history")

# Filtramos solo los registros válidos que tengan watch_id
df_history_silver = df_history_bronze.filter(col("watch_id").isNotNull())

# Guardamos en la Capa Silver en formato Delta (Output esperado Paso 6)
df_history_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("plataforma_streaming.default.silver_streaming_watch_history")

print("-> Tabla 'silver_streaming_watch_history' creada con éxito.")
print("--- ¡Capa Silver Finalizada con Éxito! ---")