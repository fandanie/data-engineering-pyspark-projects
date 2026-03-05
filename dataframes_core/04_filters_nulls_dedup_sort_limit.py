# 04_filters_nulls_dedup_sort_limit.ipynb
# Semana 2 - filtros, nulos, duplicados, sorting/limit seguro

from pyspark.sql import functions as F

df = spark.table("pyspark_course.silver_orders")
df.show(truncate=False)

# 1) Filtros (where == filter)
df_valid = df.where(~F.col("bad_amount") & ~F.col("bad_date"))
df_valid.show(truncate=False)

# 2) isin (IN SQL)
df_cities = df_valid.filter(F.col("city").isin("Bogota", "Cali"))
df_cities.show(truncate=False)

# 3) like (patrón simple)
df_like = df_valid.filter(F.col("customer").like("D%"))  # empieza por D
df_like.show(truncate=False)

# 4) rlike (regex)
df_rlike = df_valid.filter(F.col("customer").rlike("(?i)^da"))  # empieza por "da", case-insensitive
df_rlike.show(truncate=False)

# 5) Contar nulos por columna (True/False -> int -> sum)
df.select(
    F.sum(F.col("customer").isNull().cast("int")).alias("null_customer"),
    F.sum(F.col("order_date").isNull().cast("int")).alias("null_order_date"),
    F.sum(F.col("amount").isNull().cast("int")).alias("null_amount"),
    F.sum(F.col("status").isNull().cast("int")).alias("null_status"),
).show()

# 6) fillna (rellenar nulos)
df_filled = df.fillna({"status": "UNKNOWN", "customer": "UNKNOWN"})
df_filled.show(truncate=False)

# 7) dropna (eliminar filas con nulos en columnas críticas)
df_no_nulls = df.dropna(subset=["order_date", "amount"])
df_no_nulls.show(truncate=False)

# 8) Duplicados: detectar por order_id
df.groupBy("order_id").count().filter(F.col("count") > 1).show()

# 9) Quitar duplicados
df_dedup = df.dropDuplicates(["order_id"])
df_dedup.show(truncate=False)

# 10) Sorting + limit seguro
top2 = df_valid.orderBy(F.col("amount").desc()).limit(2)
top2.show(truncate=False)

# 11) collect solo porque es pequeño (top 2)
top2_list = top2.collect()
top2_list