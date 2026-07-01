"""Escritor a Delta. Materializa el resultado de cada query."""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col


class BronzeLoader:

    def __init__(self, spark: SparkSession, volume_path: str, run_id: str):
        self.spark = spark
        self.volume_path = volume_path
        self.run_id = run_id

    def write_parquet_intermedio(self, df: DataFrame, nombre_tabla: str) -> str:
        """Persiste el resultado como Parquet en el volume (trazabilidad)."""
        path = f"{self.volume_path}/{nombre_tabla}/{self.run_id}"
        df.write.format("parquet").mode("overwrite").save(path)
        return path

    def full_overwrite(self, catalog: str, schema: str, tabla: str,
                       df: DataFrame) -> int:
        """
        TRUNCATE + INSERT atómico vía insertInto(overwrite=True).

        La tabla destino YA existe (creada con DDL explícito en Sección 2.3).
        Como el extractor (JDBC) + createDataFrame infieren tipos desde filas
        Python (no desde el DDL Oracle), se castea cada columna al tipo declarado
        de la tabla destino ANTES de insertar. Así DECIMAL(20,6)/DECIMAL(15,3)
        quedan garantizados y insertInto no falla por mismatch de tipos.
        """
        full = f"{catalog}.{schema}.{tabla}"

        df_aud = (df
                  .withColumn("_ingestion_timestamp", current_timestamp())
                  .withColumn("_run_id", lit(self.run_id)))

        # Castea y reordena el df al schema exacto de la tabla destino.
        tabla_schema = self.spark.table(full).schema
        df_casteado = df_aud.select(*[
            col(f.name).cast(f.dataType).alias(f.name)
            for f in tabla_schema
        ])

        df_casteado.write.insertInto(full, overwrite=True)
        return self.spark.table(full).count()
