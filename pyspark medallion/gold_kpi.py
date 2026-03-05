# ============================================================
# 4.2 GOLD: KPIs por status
# ============================================================

gold_status = (
    df_silver
    .groupBy("status")
    .agg(
        F.count("*").alias("orders_count"),
        # Sumar solo montos válidos (si bad_amount=True suma 0.0)
        F.sum(
            F.when(~F.col("bad_amount"), F.col("amount"))
             .otherwise(F.lit(0.0))
        ).alias("total_amount_valid_only")
    )
    .orderBy(F.col("orders_count").desc())
)

gold_status.show(truncate=False)

gold_status.write.mode("overwrite").saveAsTable("pyspark_course.gold_kpi_status")