# ============================================================
# 4) GOLD: KPIs listos para consumo
#    4.1 KPIs por ciudad (count, sum, avg) usando solo registros válidos
# ============================================================

from pyspark.sql import functions as F

df_silver = spark.table("pyspark_course.silver_orders")

gold_city = (
    df_silver
    # Solo datos buenos (sin errores de monto ni fecha)
    .filter(~F.col("bad_amount") & ~F.col("bad_date"))
    .groupBy("city")
    .agg(
        F.count("*").alias("orders_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount")
    )
    .orderBy(F.col("total_amount").desc())
)

gold_city.show(truncate=False)

gold_city.write.mode("overwrite").saveAsTable("pyspark_course.gold_kpi_city")