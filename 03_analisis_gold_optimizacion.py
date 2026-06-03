# Databricks notebook source
from pyspark.sql.functions import count, col

print("--- Iniciando Creación de la Capa Gold ---")

# 1. Leemos las tablas necesarias
df_history = spark.table("plataforma_streaming.default.silver_streaming_watch_history")
df_profiles = spark.table("plataforma_streaming.default.bronze_streaming_profiles")
df_users = spark.table("plataforma_streaming.default.silver_streaming_users")

# 2. Doble Join en cadena especificando alias claros
# Unimos Historial con Perfiles
df_step1 = df_history.join(df_profiles, "profile_id", "inner")

# Unimos el resultado con Usuarios (Usamos alias o dataframes para evitar ambigüedad)
df_gold_completo = df_step1.join(df_users, "user_id", "inner")

# 3. Agrupamos especificando explícitamente que queremos el 'country' de la tabla de usuarios
df_gold_final = df_gold_completo.groupBy(df_users.country) \
                                .agg(count("watch_id").alias("total_reproducciones"))

# 4. Guardamos el resultado analítico consolidado en la Capa Gold como Tabla Delta
df_gold_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("plataforma_streaming.default.gold_reproducciones_por_pais")

print("--- ¡Capa Gold creada y guardada con éxito en formato Delta! ---")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- === PASO 7: QUERY SQL EN EL NOTEBOOK ===
# MAGIC -- Consultamos la tabla Gold ordenada de mayor a menor consumo
# MAGIC SELECT country, total_reproducciones 
# MAGIC FROM plataforma_streaming.default.gold_reproducciones_por_pais
# MAGIC ORDER BY total_reproducciones DESC;
# MAGIC
# MAGIC -- === PASO 8: OPTIMIZACIÓN DELTA (Z-ORDERING Y VACUUM) ===
# MAGIC -- 1. Z-Ordering: Organiza físicamente los archivos en base al profile_id
# MAGIC OPTIMIZE plataforma_streaming.default.silver_streaming_watch_history
# MAGIC ZORDER BY (profile_id);
# MAGIC
# MAGIC -- 2. Vacuum Serverless: Limpieza automatizada y segura respetando los 7 días de resguardo analítico
# MAGIC VACUUM plataforma_streaming.default.silver_streaming_watch_history;
# MAGIC VACUUM plataforma_streaming.default.gold_reproducciones_por_pais;