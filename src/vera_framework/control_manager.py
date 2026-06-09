"""Gestor de las tablas de control."""
from __future__ import annotations
from typing import Optional
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Row


class ControlManager:

    def __init__(self, spark: SparkSession, tabla_control: str, tabla_log: str, job_name: str):
        self.spark = spark
        self.tabla_control = tabla_control
        self.tabla_log = tabla_log
        self.job_name = job_name

    def get_cargas_activas(self) -> list[Row]:
        return self.spark.sql(f"""
            SELECT * FROM {self.tabla_control}
            WHERE activa = TRUE
            AND job_name = '{self.job_name}'
            ORDER BY orden_ejecucion NULLS LAST, id_carga
        """).collect()

    def iniciar_carga(self, id_carga: int, tabla_destino: str, query_key: str,
                      run_id: str, usuario_ejecutor: str) -> int:
        ahora = datetime.now(timezone.utc)
        df = self.spark.createDataFrame(
            [(id_carga, tabla_destino, query_key, run_id, ahora,
              "INICIADO", usuario_ejecutor)],
            schema="id_carga BIGINT, tabla_destino STRING, query_key STRING, "
                   "run_id STRING, fecha_inicio TIMESTAMP, estado STRING, "
                   "usuario_ejecutor STRING",
        )
        df.write.mode("append").saveAsTable(self.tabla_log)
        return int(self.spark.sql(f"""
            SELECT MAX(id_log) AS id FROM {self.tabla_log}
            WHERE id_carga = {id_carga} AND run_id = '{run_id}'
        """).collect()[0]["id"])

    def cerrar_exitosa(self, id_log: int, filas_leidas: int,
                       filas_escritas: int, parquet_path: str) -> None:
        self.spark.sql(f"""
            UPDATE {self.tabla_log}
            SET fecha_fin = CURRENT_TIMESTAMP(),
                duracion_segundos = UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                                    - UNIX_TIMESTAMP(fecha_inicio),
                estado = 'EXITOSO',
                filas_leidas = {filas_leidas},
                filas_escritas = {filas_escritas},
                parquet_path = '{parquet_path}'
            WHERE id_log = {id_log}
        """)

    def cerrar_fallida(self, id_log: int, mensaje_error: str) -> None:
        msg = mensaje_error.replace("'", "''")[:4000]
        self.spark.sql(f"""
            UPDATE {self.tabla_log}
            SET fecha_fin = CURRENT_TIMESTAMP(),
                duracion_segundos = UNIX_TIMESTAMP(CURRENT_TIMESTAMP())
                                    - UNIX_TIMESTAMP(fecha_inicio),
                estado = 'FALLIDO',
                mensaje_error = '{msg}'
            WHERE id_log = {id_log}
        """)