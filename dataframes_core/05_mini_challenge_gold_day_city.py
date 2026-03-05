# 05_mini_challenge_gold_day_city.ipynb
# Semana 2 - Mini reto: Gold KPI por día y ciudad

from pyspark.sql import functions as F

# 1) Cargar Silver
df = spark.table("pyspark_course.silver_orders")

# 2) Dataset base (filter + select + columnas derivadas)
base = (
    df
    .filter(~F.col("bad_amount") & ~F.col("bad_date"))
    .select(
        F.col("order_date"),
        F.upper(F.trim(F.col("city"))).alias("city_clean"),
        F.col("amount"),
        (F.col("amount") >= F.lit(1000)).alias("is_high_value")
    )
)

base.show(truncate=False)

# 3) Agregaciones (groupBy + agg)
gold_day_city = (
    base
    .groupBy("order_date", "city_clean")
    .agg(
        F.count("*").alias("orders_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.sum(F.col("is_high_value").cast("int")).alias("high_value_orders")
    )
    .orderBy(F.col("order_date").asc(), F.col("total_amount").desc())
)

gold_day_city.show(truncate=False)

# 4) Guardar tabla Gold
gold_day_city.write.mode("overwrite").saveAsTable("pyspark_course.gold_kpi_day_city")

# 5) Validar guardado
spark.table("pyspark_course.gold_kpi_day_city").show(truncate=False)
print("Rows in gold_kpi_day_city:", spark.table("pyspark_course.gold_kpi_day_city").count())