# Databricks notebook source
# MAGIC %md # 02 - Seed de midas_control_cargas (Fase A)
# MAGIC
# MAGIC Inserta/actualiza las 2 filas de control (Q1 y Q2). Parametrizado por
# MAGIC widgets: cambia catalog/schema segun el ambiente donde lo ejecutes.

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
CATALOG = dbutils.widgets.get("catalog_destino")
SCHEMA  = dbutils.widgets.get("schema_destino")
TABLA_CONTROL = f"{CATALOG}.{SCHEMA}.midas_control_cargas"
print(f"Sembrando control en: {TABLA_CONTROL}")

# COMMAND ----------
# ───── FILA 1: Q1 — Promedio Subcategoria ─────
spark.sql(f"""
    MERGE INTO {TABLA_CONTROL} t
    USING (SELECT
        '{CATALOG}'                       AS catalog_destino,
        '{SCHEMA}'                        AS schema_destino,
        'vera_promedio_subcategoria'     AS tabla_destino,
        'QUERY_FULL_OVERWRITE'            AS tipo_carga,
        'q1_promedio_subcategoria'        AS query_key,
        TRUE                              AS activa,
        10                                AS orden_ejecucion,
        'Promedio por subcategoria. Snapshot diario de ordenes de calidad activas.'
                                          AS comentarios
    ) s
    ON t.tabla_destino = s.tabla_destino
    WHEN MATCHED THEN UPDATE SET
        tipo_carga = s.tipo_carga, query_key = s.query_key,
        activa = s.activa, orden_ejecucion = s.orden_ejecucion,
        fecha_modificacion = CURRENT_TIMESTAMP(), comentarios = s.comentarios
    WHEN NOT MATCHED THEN INSERT
        (catalog_destino, schema_destino, tabla_destino, tipo_carga,
         query_key, activa, orden_ejecucion, comentarios)
        VALUES (s.catalog_destino, s.schema_destino, s.tabla_destino,
                s.tipo_carga, s.query_key, s.activa, s.orden_ejecucion,
                s.comentarios)
""")

# COMMAND ----------
# ───── FILA 2: Q2 — Promedio Individual 6 meses ─────
spark.sql(f"""
    MERGE INTO {TABLA_CONTROL} t
    USING (SELECT
        '{CATALOG}'                       AS catalog_destino,
        '{SCHEMA}'                        AS schema_destino,
        'vera_promedio_individual_6m'    AS tabla_destino,
        'QUERY_FULL_OVERWRITE'            AS tipo_carga,
        'q2_promedio_individual_6m'       AS query_key,
        TRUE                              AS activa,
        20                                AS orden_ejecucion,
        'Promedio individual 6 meses. Snapshot diario de ordenes de calidad activas.'
                                          AS comentarios
    ) s
    ON t.tabla_destino = s.tabla_destino
    WHEN MATCHED THEN UPDATE SET
        tipo_carga = s.tipo_carga, query_key = s.query_key,
        activa = s.activa, orden_ejecucion = s.orden_ejecucion,
        fecha_modificacion = CURRENT_TIMESTAMP(), comentarios = s.comentarios
    WHEN NOT MATCHED THEN INSERT
        (catalog_destino, schema_destino, tabla_destino, tipo_carga,
         query_key, activa, orden_ejecucion, comentarios)
        VALUES (s.catalog_destino, s.schema_destino, s.tabla_destino,
                s.tipo_carga, s.query_key, s.activa, s.orden_ejecucion,
                s.comentarios)
""")

# COMMAND ----------
display(spark.sql(f"""
    SELECT tabla_destino, tipo_carga, query_key, activa, orden_ejecucion
    FROM {TABLA_CONTROL} ORDER BY orden_ejecucion
"""))
