# Databricks notebook source
# 1. Definimos la lista con los nombres exactos de tus archivos (sin el .csv)
datasets = [
    "streaming_content", "streaming_content_genres", "streaming_devices", 
    "streaming_episodes", "streaming_genres", "streaming_profiles", 
    "streaming_ratings", "streaming_recommendations", "streaming_reviews", 
    "streaming_series", "streaming_subscription_plans", "streaming_subscriptions", 
    "streaming_users", "streaming_watch_history", "streaming_watch_progress", "streaming_watchlists"
]

# Ruta base de tu volumen en Unity Catalog
ruta_volumen = "/Volumes/plataforma_streaming/default/plataheim_streaming" 
# Nota: La ruta exacta la toma Spark anteponiendo /Volumes/

from pyspark.sql.functions import current_timestamp

# 2. Bucle para procesar e ingestar las 16 tablas automáticamente
for tabla in datasets:
    print(f"Procesando e ingesta de: {tabla}...")
    
    # Leer el archivo CSV desde el volumen
    df_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(f"/Volumes/plataforma_streaming/default/plataforma_streaming/{tabla}.csv")
    
    # Agregamos una columna de auditoría (Buena práctica de Data Engineering)
    df_bronze = df_raw.withColumn("fecha_ingesta", current_timestamp())
    
    # Guardar en formato Delta dentro de tu esquema default
    df_bronze.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"plataforma_streaming.default.bronze_{tabla}")

print("--- ¡Inicilización y carga a Capa Bronze completada con éxito! ---")

# COMMAND ----------

# Cargamos una de las tablas clave para mirarla
df_usuarios = spark.table("plataforma_streaming.default.bronze_streaming_users")

# El display de Databricks te genera la tabla interactiva
display(df_usuarios)