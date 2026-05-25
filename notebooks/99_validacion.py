# Databricks notebook source
# MAGIC %md # Validación post-carga

# COMMAND ----------
CATALOG, SCHEMA = "epm_datalabs_catalog_dllo", "facturacion"

# COMMAND ----------
# ───── Chequeo 1: ambas tablas tienen datos ─────
for tbl in ["vera_promedio_subcategoria", "vera_promedio_individual_6m"]:
    df = spark.table(f"{CATALOG}.{SCHEMA}.{tbl}")
    n = df.count()
    assert n > 0, f"{tbl} quedó vacía — ¿la query devolvió 0 filas?"
    print(f"✓ {tbl}: {n:,} filas")
    df.show(5, truncate=False)

# COMMAND ----------
# ───── Chequeo 2: schema correcto en Q1 ─────
cols_q1 = set(spark.table(f"{CATALOG}.{SCHEMA}.vera_promedio_subcategoria").columns)
assert {"PROM", "SESUNUSE", "ORCRPECO", "ORCRTICO",
        "_ingestion_timestamp", "_run_id"} <= cols_q1, f"Q1 cols: {cols_q1}"

cols_q2 = set(spark.table(f"{CATALOG}.{SCHEMA}.vera_promedio_individual_6m").columns)
assert {"HCPPCOPR", "SESUNUSE", "HCPPPECO", "HCPPTICO",
        "_ingestion_timestamp", "_run_id"} <= cols_q2, f"Q2 cols: {cols_q2}"
print("✓ Schemas correctos")

# COMMAND ----------
# ───── Chequeo 3: SESUNUSE sin nulos (es la clave) ─────
for tbl in ["vera_promedio_subcategoria", "vera_promedio_individual_6m"]:
    nulos = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.{tbl}
        WHERE SESUNUSE IS NULL
    """).collect()[0]["n"]
    assert nulos == 0, f"{tbl}: {nulos} filas con SESUNUSE NULL"
print("✓ SESUNUSE sin nulos en ambas tablas")

# COMMAND ----------
# ───── Chequeo 4: el log refleja éxito ─────
display(spark.sql(f"""
    SELECT id_log, tabla_destino, query_key, estado,
           duracion_segundos, filas_escritas, fecha_inicio
    FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    ORDER BY fecha_inicio DESC LIMIT 10
"""))

ultimos_fallidos = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {CATALOG}.{SCHEMA}.midas_log_cargas
    WHERE estado = 'FALLIDO'
      AND fecha_inicio >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
""").collect()[0]["n"]
assert ultimos_fallidos == 0, f"{ultimos_fallidos} cargas fallidas en la última hora"

# COMMAND ----------
print("\n=== VALIDACIÓN COMPLETA ===")