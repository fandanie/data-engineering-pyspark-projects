# ============================================================
# 2) BRONZE: guardar datos crudos como TABLA (managed table)
#    (porque DBFS root está bloqueado en tu entorno)
# ============================================================

# Crear base de datos (schema) si no existe
spark.sql("CREATE DATABASE IF NOT EXISTS pyspark_course")

# Guardar Bronze
df_raw.write.mode("overwrite").saveAsTable("pyspark_course.bronze_orders")

# Leer y validar Bronze
df_bronze = spark.table("pyspark_course.bronze_orders")
df_bronze.show(truncate=False)
df_bronze.printSchema()