# 03_select_alias_common_functions.ipynb
# Semana 2 - select vs withColumn + alias + funciones comunes

from pyspark.sql import functions as F

# 1) Base: Silver -> df2 (solo válidos)
df = spark.table("pyspark_course.silver_orders")
df2 = df.filter(~F.col("bad_amount") & ~F.col("bad_date"))

# 2) Construir dataset final SOLO con select (recomendado para capas Silver/Gold)
df_final = df2.select(
    F.col("order_id").alias("order_id"),
    F.col("customer").alias("customer_name"),
    F.upper(F.trim(F.col("city"))).alias("city_clean"),
    F.col("order_date").alias("order_date"),
    F.col("amount").alias("amount"),
    F.round(F.col("amount"), 0).alias("amount_round"),
    (F.col("amount") >= F.lit(1000)).alias("is_high_value"),
    F.when(F.col("amount") >= 1000, F.lit("HIGH"))
     .when(F.col("amount") >= 500, F.lit("MID"))
     .otherwise(F.lit("LOW"))
     .alias("amount_bucket")
)

df_final.show(truncate=False)
df_final.printSchema()

# 3) explain para ver "Project" (select suele verse como Project)
df_final.explain()

# 4) Funciones comunes

# 4.1 coalesce: primer valor no nulo
df_demo = df2.select(
    "order_id",
    F.coalesce(F.col("customer"), F.lit("UNKNOWN")).alias("customer_filled")
)
df_demo.show(truncate=False)

# 4.2 concat + lit: crear strings
df_demo2 = df2.select(
    "order_id",
    F.concat(F.lit("ORDER-"), F.col("order_id")).alias("order_code")
)
df_demo2.show(truncate=False)

# 4.3 regexp_replace: limpiar texto (solo letras y espacios)
df_demo3 = df2.select(
    "order_id",
    F.regexp_replace(F.col("customer"), r"[^A-Za-z\s]", "").alias("customer_only_letters")
)
df_demo3.show(truncate=False)