"""Configuración inmutable de una ejecución del framework."""
from dataclasses import dataclass


@dataclass(frozen=True)
class FrameworkConfig:
    ambiente: str
    catalog_destino: str
    schema_destino: str
    volume_path: str

    oracle_secret_scope: str
    oracle_user: str            # usuario Oracle: va explícito, NO está en el secret scope
    oracle_password_key: str
    oracle_host: str
    oracle_port: int
    oracle_service: str

    run_id: str
    usuario_ejecutor: str

    @property
    def tabla_control(self) -> str:
        return f"{self.catalog_destino}.{self.schema_destino}.midas_control_cargas"

    @property
    def tabla_log(self) -> str:
        return f"{self.catalog_destino}.{self.schema_destino}.midas_log_cargas"

    def tabla_destino(self, nombre: str) -> str:
        return f"{self.catalog_destino}.{self.schema_destino}.{nombre}"

    @property
    def oracle_dsn(self) -> str:
        """DSN para python-oracledb: 'host:puerto/servicio'."""
        return f"{self.oracle_host}:{self.oracle_port}/{self.oracle_service}"

    @classmethod
    def from_job_params(cls, dbutils, run_id: str) -> "FrameworkConfig":
        g = dbutils.widgets.get
        return cls(
            ambiente=g("ambiente"),
            catalog_destino=g("catalog_destino"),
            schema_destino=g("schema_destino"),
            volume_path=g("volume_path"),
            oracle_secret_scope=g("oracle_secret_scope"),
            oracle_user=g("oracle_user"),
            oracle_password_key=g("oracle_password_key"),
            oracle_host=g("oracle_host"),
            oracle_port=int(g("oracle_port")),
            oracle_service=g("oracle_service"),
            run_id=run_id,
            usuario_ejecutor=(dbutils.notebook.entry_point.getDbutils()
                              .notebook().getContext().userName().get()),
        )
