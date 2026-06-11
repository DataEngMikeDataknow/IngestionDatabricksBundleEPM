# Databricks notebook source
# MAGIC %md # 00 - Creacion de objetos del framework (DDL / migraciones)
# MAGIC
# MAGIC Crea de forma **idempotente** (`CREATE TABLE IF NOT EXISTS`) las tablas que
# MAGIC el framework necesita: control, log y las 2 tablas destino. Es el **primer
# MAGIC task** del job (`ejecutar_framework` depende de este), asi que corre en cada
# MAGIC ejecucion: por eso debe ser idempotente (no recrea ni borra nada existente).
# MAGIC
# MAGIC Parametrizado por `catalog_destino`/`schema_destino` para servir a
# MAGIC dllo/uat/pdn con el mismo codigo. Hace dos cosas:
# MAGIC 1. **Estructura (DDL):** crea control, log y las 2 tablas destino.
# MAGIC 2. **Bootstrap del control:** siembra las filas de config que el framework
# MAGIC    lee para saber que cargar, con semantica *insertar si falta* (no pisa
# MAGIC    ediciones manuales). Asi uat/pdn quedan operativos sin pasos manuales.
# MAGIC
# MAGIC > El notebook `02_seed_control_cargas.py` queda como utilitario manual
# MAGIC > (reseed/ajustes y filas de otros jobs); el bootstrap minimo ya vive aqui.
# MAGIC
# MAGIC > Requiere que el principal de ejecucion tenga `USE CATALOG`, `USE SCHEMA` y
# MAGIC > `CREATE TABLE` sobre el schema destino.

# COMMAND ----------
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
CATALOG = dbutils.widgets.get("catalog_destino")
SCHEMA  = dbutils.widgets.get("schema_destino")
print(f"Creando/validando objetos en: {CATALOG}.{SCHEMA}")


def crear_tabla(ddl: str, full: str, comentario: str) -> None:
    """Ejecuta el DDL (idempotente) y, best-effort, fija el COMMENT.
    El COMMENT se aisla en try/except: si la tabla ya existe y el principal no
    es owner, setear el comentario puede fallar y no debe romper el task."""
    spark.sql(ddl)
    try:
        spark.sql(f"COMMENT ON TABLE {full} IS '{comentario}'")
    except Exception as e:  # noqa: BLE001
        print(f"[warn] no se pudo fijar COMMENT en {full}: {e}")
    print(f"OK  {full}")

# COMMAND ----------
# ───── Tabla de CONTROL ─────
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.midas_control_cargas (
        id_carga            BIGINT GENERATED ALWAYS AS IDENTITY,
        catalog_destino     STRING  NOT NULL,
        schema_destino      STRING  NOT NULL,
        tabla_destino       STRING  NOT NULL,
        tipo_carga          STRING  NOT NULL,
        query_key           STRING  NOT NULL,
        job_name            STRING,                  -- job dueño de la carga; el framework filtra por esto
        activa              BOOLEAN NOT NULL,
        orden_ejecucion     INT,
        query_padre_id           BIGINT,
        columna_join             STRING,
        campo_filtro_incremental STRING,
        fecha_creacion      TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        fecha_modificacion  TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        comentarios         STRING
    )
    USING DELTA
    TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
    """,
    f"{CATALOG}.{SCHEMA}.midas_control_cargas",
    "Configuracion del framework metadata-driven. Una fila por cada query materializada.",
)

# COMMAND ----------
# ───── Tabla de LOG ─────
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.midas_log_cargas (
        id_log              BIGINT GENERATED ALWAYS AS IDENTITY,
        id_carga            BIGINT  NOT NULL,
        tabla_destino       STRING,
        query_key           STRING,
        run_id              STRING,
        fecha_inicio        TIMESTAMP NOT NULL,
        fecha_fin           TIMESTAMP,
        duracion_segundos   DOUBLE,
        estado              STRING,
        filas_leidas        BIGINT,
        filas_escritas      BIGINT,
        parquet_path        STRING,
        mensaje_error       STRING,
        usuario_ejecutor    STRING
    )
    USING DELTA
    """,
    f"{CATALOG}.{SCHEMA}.midas_log_cargas",
    "Bitacora de ejecuciones del framework. Una fila por intento de carga.",
)

# COMMAND ----------
# ───── Tabla destino Q1: vera_promedio_subcategoria ─────
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.vera_promedio_subcategoria (
        PROM        DECIMAL(20,6),
        SESUNUSE    BIGINT  NOT NULL,
        ORCRPECO    BIGINT  NOT NULL,
        ORCRTICO    INT     NOT NULL,
        _ingestion_timestamp TIMESTAMP,
        _run_id     STRING
    )
    USING DELTA
    """,
    f"{CATALOG}.{SCHEMA}.vera_promedio_subcategoria",
    "Resultado diario de la query Q1 (Promedio Subcategoria). Snapshot de ordenes de calidad activas.",
)

# COMMAND ----------
# ───── Tabla destino Q2: vera_promedio_individual_6m ─────
crear_tabla(
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}.vera_promedio_individual_6m (
        HCPPCOPR    DECIMAL(15,3),
        SESUNUSE    BIGINT  NOT NULL,
        HCPPPECO    BIGINT,
        HCPPTICO    INT,
        _ingestion_timestamp TIMESTAMP,
        _run_id     STRING
    )
    USING DELTA
    """,
    f"{CATALOG}.{SCHEMA}.vera_promedio_individual_6m",
    "Resultado diario de la query Q2 (Promedio Individual 6 meses). Snapshot de ordenes de calidad activas.",
)

# COMMAND ----------
# ───── Seed (bootstrap) de la tabla de CONTROL ─────
# Crea las filas de configuracion que el framework lee para saber QUE cargar.
# Semantica "insertar si falta" (solo WHEN NOT MATCHED): es idempotente y seguro
# de correr en cada ejecucion del job, SIN pisar ediciones manuales. Si un
# operador edita una fila existente (p. ej. activa=FALSE para desactivar una
# carga), el bootstrap NO la sobreescribe. Para cambiar la config de una fila
# que ya existe se edita la tabla de control directamente.
CARGAS_BOOTSTRAP = [
    {
        "tabla_destino":   "vera_promedio_subcategoria",
        "tipo_carga":      "QUERY_FULL_OVERWRITE",
        "query_key":       "q1_promedio_subcategoria",
        "orden_ejecucion": 10,
        "comentarios":     "Promedio por subcategoria. Snapshot diario de ordenes de calidad activas.",
    },
    {
        "tabla_destino":   "vera_promedio_individual_6m",
        "tipo_carga":      "QUERY_FULL_OVERWRITE",
        "query_key":       "q2_promedio_individual_6m",
        "orden_ejecucion": 20,
        "comentarios":     "Promedio individual 6 meses. Snapshot diario de ordenes de calidad activas.",
    },
]

control = f"{CATALOG}.{SCHEMA}.midas_control_cargas"
for c in CARGAS_BOOTSTRAP:
    spark.sql(f"""
        MERGE INTO {control} t
        USING (SELECT
            '{CATALOG}'              AS catalog_destino,
            '{SCHEMA}'               AS schema_destino,
            '{c["tabla_destino"]}'   AS tabla_destino,
            '{c["tipo_carga"]}'      AS tipo_carga,
            '{c["query_key"]}'       AS query_key,
            'vera_framework'         AS job_name,
            TRUE                     AS activa,
            {c["orden_ejecucion"]}   AS orden_ejecucion,
            '{c["comentarios"]}'     AS comentarios
        ) s
        ON t.tabla_destino = s.tabla_destino
        WHEN NOT MATCHED THEN INSERT
            (catalog_destino, schema_destino, tabla_destino, tipo_carga,
             query_key, job_name, activa, orden_ejecucion, comentarios)
            VALUES (s.catalog_destino, s.schema_destino, s.tabla_destino,
                    s.tipo_carga, s.query_key, s.job_name, s.activa,
                    s.orden_ejecucion, s.comentarios)
    """)
    print(f"OK  control bootstrap (insert-if-missing): {c['tabla_destino']}")

# COMMAND ----------
# ───── Verificacion: las 4 tablas del framework existen ─────
for tbl in ["midas_control_cargas", "midas_log_cargas",
            "vera_promedio_subcategoria", "vera_promedio_individual_6m"]:
    full = f"{CATALOG}.{SCHEMA}.{tbl}"
    assert spark.catalog.tableExists(full), f"FALTA: {full}"
    print(f"OK  existe {full}")

# COMMAND ----------
# ───── Verificacion: filas de control activas para vera_framework ─────
# Si esto sale vacio, el job no cargaria nada.
filas_control = spark.sql(f"""
    SELECT tabla_destino, tipo_carga, query_key, activa, orden_ejecucion
    FROM {CATALOG}.{SCHEMA}.midas_control_cargas
    WHERE job_name = 'vera_framework'
    ORDER BY orden_ejecucion
""")
assert filas_control.count() > 0, \
    "La tabla de control no tiene filas para job_name='vera_framework' (el job no cargaria nada)"
display(filas_control)

# COMMAND ----------
print("\n=== OBJETOS DEL FRAMEWORK CREADOS / VALIDADOS ===")
