# 01_transformations_vs_actions.ipynb
# Semana 2 - Transformations vs Actions + explain

from pyspark.sql import functions as F

# 1) Cargar Silver
df = spark.table("pyspark_course.silver_orders")
df.show(truncate=False)
df.printSchema()

# 2) TRANSFORMATION (no ejecuta todavía)
df2 = df.filter(~F.col("bad_amount") & ~F.col("bad_date"))

# 3) Ver el plan (útil para performance/debug)
df2.explain()

# Opcional: formato más legible (si está disponible en tu runtime)
# df2.explain("formatted")

# 4) ACTION (esto sí ejecuta en el cluster)
valid_count = df2.count()
print("Valid rows (df2.count):", valid_count)

# 5) Otra action (muestra filas; internamente ejecuta un job)
df2.show(truncate=False)