# ============================================================
# 1) CREAR DATASET RAW (con problemas reales)
# ============================================================

raw_data = [
  ("1001","Daniel","2026-01-05","1200.50","Bogota","  ACTIVE  "),
  ("1002","Ana","2026/01/06","900","Medellin",None),
  ("1003","Luis","05-01-2026","not_a_number","Cali","ACTIVE"),
  ("1003","Luis","05-01-2026","not_a_number","Cali","ACTIVE"),  # duplicado
  ("1004","Sofi",None,"450.25","Barranquilla","inactive"),
]

columns = ["order_id","customer","order_date","amount","city","status"]

df_raw = spark.createDataFrame(raw_data, columns)
df_raw.show(truncate=False)
df_raw.printSchema()