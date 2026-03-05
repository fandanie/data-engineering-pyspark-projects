# ============================================================
# 0) VALIDACIÓN DEL ENTORNO (Databricks Serverless)
# ============================================================

# Ver versión de Spark
spark.version

# Ver usuario actual (permiso/identidad)
spark.sql("SELECT current_user() AS user").show(truncate=False)

# Ver algunas configuraciones del runtime (para entender modo ANSI, timezone, etc.)
spark.sql("SET -v").limit(10).show(truncate=False)