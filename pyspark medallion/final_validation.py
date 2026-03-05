# ============================================================
# 5) VALIDACIONES FINALES (opcionales)
# ============================================================

# Ver tablas creadas
spark.sql("SHOW TABLES IN pyspark_course").show(truncate=False)

# Ver Silver final
spark.table("pyspark_course.silver_orders").show(truncate=False)

# Ver Gold final
spark.table("pyspark_course.gold_kpi_city").show(truncate=False)
spark.table("pyspark_course.gold_kpi_status").show(truncate=False)