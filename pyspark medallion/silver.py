# ============================================================
# 3) SILVER: limpieza + tipado + QC flags + deduplicación
# ============================================================

from pyspark.sql import functions as F

df_bronze = spark.table("pyspark_course.bronze_orders")

df_clean = (
    df_bronze
    # Normalizar status: trim (quita espacios) + upper (mayúsculas)
    .withColumn("status_clean", F.upper(F.trim(F.col("status"))))

    # Convertir amount a double sin tumbar el job (ANSI mode): try_cast -> inválidos quedan NULL
    .withColumn("amount_num", F.expr("try_cast(amount as double)"))

    # Parsear fechas en múltiples formatos: try_to_date + coalesce (primer formato válido)
    .withColumn(
        "order_date_parsed",
        F.coalesce(
            F.expr("try_to_date(order_date, 'yyyy-MM-dd')"),
            F.expr("try_to_date(order_date, 'yyyy/MM/dd')"),
            F.expr("try_to_date(order_date, 'dd-MM-yyyy')")
        )
    )
)

# QC flags (quality checks): marcar lo que quedó inválido
df_qc = (
    df_clean
    .withColumn("bad_amount", F.col("amount_num").isNull())
    .withColumn("bad_date", F.col("order_date_parsed").isNull())
)

# Vista de validación
df_qc.select(
    "order_id","order_date","order_date_parsed",
    "amount","amount_num",
    "bad_amount","bad_date",
    "status","status_clean"
).show(truncate=False)

# Deduplicación + selección final de columnas Silver
df_silver = (
    df_qc
    .dropDuplicates(["order_id"])
    .select(
        "order_id",
        "customer",
        "city",
        F.col("status_clean").alias("status"),
        F.col("order_date_parsed").alias("order_date"),
        F.col("amount_num").alias("amount"),
        "bad_amount",
        "bad_date"
    )
)

df_silver.show(truncate=False)
df_silver.printSchema()

# Guardar tabla Silver
df_silver.write.mode("overwrite").saveAsTable("pyspark_course.silver_orders")