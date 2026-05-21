"""Orquestador. Itera sobre las cargas activas y despacha por tipo."""
import traceback
from pyspark.sql import SparkSession, Row

from .config import FrameworkConfig
from .oracle_extractor import OracleExtractor, OracleCredentials
from .bronze_loader import BronzeLoader
from .control_manager import ControlManager
from .queries import get_query


class MetadataOrchestrator:

    def __init__(self, spark: SparkSession, config: FrameworkConfig, dbutils):
        self.spark = spark
        self.config = config

        creds = OracleCredentials.from_secret_scope(
            dbutils=dbutils,
            scope=config.oracle_secret_scope,
            user=config.oracle_user,
            password_key=config.oracle_password_key,
            dsn=config.oracle_dsn,
        )
        self.extractor = OracleExtractor(spark, creds)
        self.loader = BronzeLoader(spark, config.volume_path, config.run_id)
        self.control = ControlManager(spark, config.tabla_control,
                                      config.tabla_log)

    def run_daily(self) -> dict:
        """Ejecuta todas las cargas activas. No aborta el lote si una falla."""
        cargas = self.control.get_cargas_activas()
        resumen = {"total": len(cargas), "exitosas": 0,
                   "fallidas": 0, "detalle": []}

        for fila in cargas:
            try:
                r = self._dispatch(fila)
                resumen["exitosas"] += 1
                resumen["detalle"].append(
                    {"tabla": fila["tabla_destino"], "estado": "EXITOSO", **r})
            except Exception as e:
                resumen["fallidas"] += 1
                resumen["detalle"].append(
                    {"tabla": fila["tabla_destino"], "estado": "FALLIDO",
                     "error": str(e)[:300]})
        return resumen

    def _dispatch(self, fila: Row) -> dict:
        tipo = fila["tipo_carga"]
        if tipo == "QUERY_FULL_OVERWRITE":
            return self._query_full_overwrite(fila)
        raise ValueError(f"tipo_carga no soportado en Fase A: {tipo}")

    def _query_full_overwrite(self, fila: Row) -> dict:
        id_log = self.control.iniciar_carga(
            id_carga=fila["id_carga"],
            tabla_destino=fila["tabla_destino"],
            query_key=fila["query_key"],
            run_id=self.config.run_id,
            usuario_ejecutor=self.config.usuario_ejecutor,
        )
        try:
            # 1. Recuperar el SQL del registro
            sql = get_query(fila["query_key"])

            # 2. Ejecutar contra Oracle (python-oracledb, en el driver)
            df = self.extractor.read_query(sql)

            # 3. Persistir Parquet (trazabilidad / rompe lineage con Oracle)
            parquet_path = self.loader.write_parquet_intermedio(
                df, fila["tabla_destino"])
            df_parquet = self.spark.read.parquet(parquet_path)
            n = df_parquet.count()

            # 4. TRUNCATE + INSERT en la tabla destino
            filas = self.loader.full_overwrite(
                catalog=fila["catalog_destino"],
                schema=fila["schema_destino"],
                tabla=fila["tabla_destino"],
                df=df_parquet,
            )

            self.control.cerrar_exitosa(
                id_log=id_log, filas_leidas=n,
                filas_escritas=filas, parquet_path=parquet_path)
            return {"filas": filas, "parquet_path": parquet_path}

        except Exception:
            self.control.cerrar_fallida(id_log, traceback.format_exc())
            raise
