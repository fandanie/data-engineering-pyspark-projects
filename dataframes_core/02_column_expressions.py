# 02_column_expressions.ipynb
# Semana 2 - Columnas derivadas con withColumn + when/otherwise + rename/drop

from pyspark.sql import functions as F

# 1) Base: Silver -> df2 (solo válidos)
df = spark.table("pyspark_course.silver_orders")
df2 = df.filter(~F.col("bad_amount") & ~F.col("bad_date"))

# 2) Crear columnas nuevas con withColumn
df_cols = (
    df2
    .withColumn("city_clean", F.upper(F.trim(F.col("city"))))
    .withColumn("amount_round", F.round(F.col("amount"), 0))
    .withColumn("is_high_value", F.col("amount") >= F.lit(1000))
)

df_cols.select("order_id", "city", "city_clean", "amount", "amount_round", "is_high_value").show(truncate=False)

# 3) Columna condicional con when/otherwise (bucket)
df_buckets = (
    df_cols
    .withColumn(
        "amount_bucket",
        F.when(F.col("amount") >= 1000, F.lit("HIGH"))
         .when(F.col("amount") >= 500, F.lit("MID"))
         .otherwise(F.lit("LOW"))
    )
)

df_buckets.select("order_id", "amount", "amount_bucket").show(truncate=False)

# 4) Renombrar columna
df_renamed = df_buckets.withColumnRenamed("order_id", "order_id_str")
df_renamed.printSchema()

# 5) Eliminar columnas
df_dropped = df_renamed.drop("bad_amount", "bad_date")
df_dropped.printSchema()