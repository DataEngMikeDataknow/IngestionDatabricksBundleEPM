"""
Extractor Oracle vía python-oracledb (driver thin).

Por qué oracledb y no Spark JDBC:
  spark.read.format("jdbc") se ejecuta como fuente de datos externa de Spark.
  En clusters Unity Catalog en modo de acceso Shared/Standard, esto requiere el
  privilegio SELECT ON ANY FILE y falla con:
    [INSUFFICIENT_PERMISSIONS] ... permission SELECT on any file. SQLSTATE: 42501
  python-oracledb corre como código Python plano en el DRIVER: abre un socket TCP
  a Oracle y trae las filas a memoria. Unity Catalog no lo intercepta.

  Si en el futuro se quisiera volver a Spark JDBC, habría que usar un cluster en
  modo Single User / Dedicated, o configurar Lakehouse Federation (Foreign Catalog).

Apto para resultados pequeños/medianos (Q1, Q2 están filtradas por órdenes activas).
fetchall() trae todo a memoria del driver; para resultados de millones de filas
habría que paginar con fetchmany().
"""
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import oracledb


@dataclass
class OracleCredentials:
    user: str
    password: str
    dsn: str            # 'host:puerto/servicio'

    @classmethod
    def from_secret_scope(cls, dbutils, scope: str, user: str,
                          password_key: str, dsn: str) -> "OracleCredentials":
        # OJO: el usuario Oracle NO está en el secret scope; se pasa explícito.
        # Solo el password se lee del scope.
        return cls(
            user=user,
            password=dbutils.secrets.get(scope=scope, key=password_key),
            dsn=dsn,
        )


class OracleExtractor:

    def __init__(self, spark: SparkSession, creds: OracleCredentials):
        self.spark = spark
        self.creds = creds

    def read_query(self, sql: str) -> DataFrame:
        """
        Ejecuta una query SQL contra Oracle con python-oracledb (modo thin).
        Soporta CTEs (WITH) y sintaxis Oracle pura.
        La lectura ocurre en el driver; el resultado se sube a un DataFrame Spark.

        El casteo final de tipos al schema de la tabla destino lo hace BronzeLoader;
        aquí solo se infieren tipos desde las tuplas Python que devuelve oracledb.
        """
        conn = oracledb.connect(
            user=self.creds.user,
            password=self.creds.password,
            dsn=self.creds.dsn,
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columnas = [d[0] for d in cur.description]
            filas = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        if not filas:
            # DataFrame vacío pero con columnas, para que insertInto no truene
            # cuando una query legítimamente devuelve 0 filas.
            schema = StructType([
                StructField(c, StringType(), True) for c in columnas
            ])
            return self.spark.createDataFrame([], schema)

        # createDataFrame infiere tipos de las tuplas Python (int, float,
        # decimal.Decimal, str). BronzeLoader castea luego al schema destino.
        return self.spark.createDataFrame(filas, schema=columnas)

    def test_connection(self) -> bool:
        """Prueba trivial de conectividad: SELECT 1 FROM DUAL."""
        conn = oracledb.connect(
            user=self.creds.user,
            password=self.creds.password,
            dsn=self.creds.dsn,
        )
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1 AS OK FROM DUAL")
            ok = cur.fetchone()[0] == 1
            cur.close()
            return ok
        finally:
            conn.close()
