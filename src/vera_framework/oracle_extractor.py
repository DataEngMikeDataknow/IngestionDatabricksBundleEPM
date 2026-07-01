"""
Extractor Oracle con backend ÚNICO: Oracle JDBC thin vía JayDeBeApi.

Se usa el mismo driver en TODOS los ambientes (dllo/uat/pdn) por reproducibilidad
y coherencia de despliegue: el comportamiento no cambia entre entornos por usar
distinto cliente. (Antes dllo/uat usaban python-oracledb thin y solo pdn JDBC;
ahora es JDBC en todos.)

Por qué Oracle JDBC (ojdbc) y no python-oracledb thin:
  El driver JDBC oficial de Oracle (ojdbc, Java puro) autentica contra cuentas con
  verificador de contraseña 10G, que python-oracledb en modo thin NO soporta
  (DPY-3015). No requiere Oracle Instant Client ni librerías nativas (libaio):
  solo el jar ojdbc en el classpath de una JVM que JPype levanta en el driver.

Por qué NO Spark JDBC (spark.read.format("jdbc")):
  En clusters Unity Catalog en modo Shared/Standard requiere SELECT ON ANY FILE y
  falla con [INSUFFICIENT_PERMISSIONS]. Aquí el JDBC corre como código plano en el
  DRIVER (JayDeBeApi abre un socket a Oracle y trae las filas a memoria), así que
  UC no lo intercepta.

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
    jdbc_jar_path: str          # ruta del jar ojdbc en el Volume (requerido)

    @classmethod
    def from_secret_scope(cls, dbutils, scope: str, user: str, password_key: str,
                          dsn: str, jdbc_jar_path: str) -> "OracleCredentials":
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
        Ejecuta una query SQL contra Oracle (JDBC/JayDeBeApi) y devuelve un
        DataFrame Spark. Soporta CTEs (WITH) y sintaxis Oracle pura. La lectura
        ocurre en el driver.

        El casteo final de tipos al schema de la tabla destino lo hace BronzeLoader;
        aquí solo se normalizan/infieren tipos desde las filas.
        """
        columnas, filas = self._fetch_jdbc(sql)

        if not filas:
            # DataFrame vacío pero con columnas, para que insertInto no truene
            # cuando una query legítimamente devuelve 0 filas.
            schema = StructType([
                StructField(c, StringType(), True) for c in columnas
            ])
            return self.spark.createDataFrame([], schema)

        return self.spark.createDataFrame(filas, schema=columnas)

    # ───── Backend: Oracle JDBC thin vía JayDeBeApi ─────
    def _fetch_jdbc(self, sql: str):
        if not self.creds.jdbc_jar_path:
            raise ValueError(
                "oracle_jdbc_jar_path no está configurado. Se requiere la ruta del "
                "jar ojdbc (en un Volume) para conectar a Oracle por JDBC/JayDeBeApi."
            )
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
        """Prueba trivial de conectividad (JDBC): SELECT 1 FROM DUAL."""
        _, filas = self._fetch_jdbc("SELECT 1 AS OK FROM DUAL")
        return bool(filas) and int(filas[0][0]) == 1
