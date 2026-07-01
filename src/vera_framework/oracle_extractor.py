"""
Extractor Oracle con DOS backends seleccionables (mismo flujo, distinto driver):

1) python-oracledb (modo thin) — POR DEFECTO (dllo/uat).
   Cliente Python puro. NO soporta verificadores de contraseña 10G: contra una
   cuenta con ese verifier lanza DPY-3015.

2) Oracle JDBC thin vía JayDeBeApi — se activa cuando creds.jdbc_jar_path != "".
   Usa el driver oficial de Oracle en Java puro (ojdbc), que SÍ autentica contra
   el verifier 10G. NO requiere Oracle Instant Client ni librerías nativas
   (libaio): solo el jar ojdbc en el classpath de una JVM que JPype levanta en el
   driver. Se usa en pdn, donde la cuenta tiene verifier 10G.

Por qué NO Spark JDBC (spark.read.format("jdbc")):
  En clusters Unity Catalog en modo Shared/Standard requiere SELECT ON ANY FILE y
  falla con [INSUFFICIENT_PERMISSIONS]. Ambos backends de aquí corren como código
  plano en el DRIVER (abren un socket a Oracle y traen las filas a memoria), así
  que UC no los intercepta. JayDeBeApi NO es el data source de Spark: es JDBC
  llamado directo desde Python en el driver, por eso no choca con esa restricción.

Apto para resultados pequeños/medianos (Q1, Q2 filtradas por órdenes activas).
fetchall() trae todo a memoria del driver; para millones de filas habría que
paginar con fetchmany().
"""
from dataclasses import dataclass
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType


@dataclass
class OracleCredentials:
    user: str
    password: str
    dsn: str                    # 'host:puerto/servicio'
    jdbc_jar_path: str = ""     # "" => python-oracledb thin; ruta de jar ojdbc => JDBC/JayDeBeApi

    @classmethod
    def from_secret_scope(cls, dbutils, scope: str, user: str, password_key: str,
                          dsn: str, jdbc_jar_path: str = "") -> "OracleCredentials":
        # OJO: el usuario Oracle NO está en el secret scope; se pasa explícito.
        # Solo el password se lee del scope.
        return cls(
            user=user,
            password=dbutils.secrets.get(scope=scope, key=password_key),
            dsn=dsn,
            jdbc_jar_path=jdbc_jar_path,
        )


class OracleExtractor:

    def __init__(self, spark: SparkSession, creds: OracleCredentials):
        self.spark = spark
        self.creds = creds

    def read_query(self, sql: str) -> DataFrame:
        """
        Ejecuta una query SQL contra Oracle y devuelve un DataFrame Spark.
        Soporta CTEs (WITH) y sintaxis Oracle pura. La lectura ocurre en el driver.

        Elige el backend según creds.jdbc_jar_path; la conversión de filas a
        DataFrame es común. El casteo final de tipos al schema de la tabla destino
        lo hace BronzeLoader; aquí solo se infieren tipos desde las filas Python.
        """
        if self.creds.jdbc_jar_path:
            columnas, filas = self._fetch_jdbc(sql)
        else:
            columnas, filas = self._fetch_oracledb(sql)

        if not filas:
            # DataFrame vacío pero con columnas, para que insertInto no truene
            # cuando una query legítimamente devuelve 0 filas.
            schema = StructType([
                StructField(c, StringType(), True) for c in columnas
            ])
            return self.spark.createDataFrame([], schema)

        # createDataFrame infiere tipos de las filas Python (int, float,
        # decimal.Decimal, str). BronzeLoader castea luego al schema destino.
        return self.spark.createDataFrame(filas, schema=columnas)

    # ───── Backend 1: python-oracledb (thin) ─────
    def _fetch_oracledb(self, sql: str):
        # Import perezoso: solo dllo/uat instalan/usan oracledb.
        import oracledb
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
            return columnas, filas
        finally:
            conn.close()

    # ───── Backend 2: Oracle JDBC thin vía JayDeBeApi ─────
    def _fetch_jdbc(self, sql: str):
        # Import perezoso: solo pdn instala/usa JayDeBeApi + JPype1.
        import jaydebeapi
        from decimal import Decimal, InvalidOperation
        url = f"jdbc:oracle:thin:@{self.creds.dsn}"   # dsn = host:puerto/servicio
        conn = jaydebeapi.connect(
            "oracle.jdbc.OracleDriver",               # clase del driver dentro del jar
            url,
            [self.creds.user, self.creds.password],
            self.creds.jdbc_jar_path,                 # jar ojdbc (en Volume); JPype lo pone en el classpath
        )
        try:
            cur = conn.cursor()
            cur.execute(sql)
            columnas = [d[0] for d in cur.description]
            escalas = [d[5] for d in cur.description]   # 'scale' del metadata DB-API
            filas_raw = cur.fetchall()
            cur.close()
        finally:
            conn.close()

        # JayDeBeApi devuelve los NUMBER de Oracle como objetos Java (JPype:
        # JDouble, BigDecimal, ...). Spark no puede inferir un tipo desde un objeto
        # Java y los materializa como 'struct' vacío => EMPTY_SCHEMA al escribir
        # Parquet. Se convierten a tipos Python nativos usando la escala del
        # metadata: scale 0 => int, scale>0 => Decimal, cualquier otro => str.
        # BronzeLoader castea luego al tipo exacto de la tabla destino.
        def _py(v, scale):
            if v is None:
                return None
            try:
                d = Decimal(str(v))
            except (InvalidOperation, ValueError):
                return str(v)
            return d if scale else int(d)

        filas = [tuple(_py(v, sc) for v, sc in zip(fila, escalas)) for fila in filas_raw]
        return columnas, filas

    def test_connection(self) -> bool:
        """Prueba trivial de conectividad por el backend activo: SELECT 1 FROM DUAL."""
        fetch = self._fetch_jdbc if self.creds.jdbc_jar_path else self._fetch_oracledb
        _, filas = fetch("SELECT 1 AS OK FROM DUAL")
        return bool(filas) and int(filas[0][0]) == 1
