# Databricks notebook source
# MAGIC %md # Entrypoint del framework metadata-driven (job diario)
# MAGIC
# MAGIC El paquete `vera_framework` y el driver `oracledb` vienen instalados
# MAGIC como librerias del **job cluster** (ver `libraries:` en
# MAGIC `databricks.yml`). Por eso este notebook **ya no**
# MAGIC necesita los parches de desarrollo (`%pip install` ni `sys.path`).

# COMMAND ----------
# Widgets: el job los rellena desde base_parameters del job.yml.
dbutils.widgets.text("ambiente", "dllo")
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
dbutils.widgets.text("volume_path",
    "/Volumes/epm_datalake_vol_np/facturacion_vol/facturacion_bronze_vol/vera_framework/dllo")
dbutils.widgets.text("oracle_secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-DLLO")
dbutils.widgets.text("oracle_user", "SQL_EPMBOTPD05")
dbutils.widgets.text("oracle_password_key", "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA")
dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co")
dbutils.widgets.text("oracle_port", "1521")
dbutils.widgets.text("oracle_service", "SFUAT")

# COMMAND ----------
# Fallback SOLO para ejecucion INTERACTIVA manual (notebook adjunto a un
# cluster, no como tarea del job). Cuando corre como job, el wheel ya esta
# instalado y este bloque no hace nada.
#
# Nota: si ejecutas a mano en un cluster sin oracledb, instalalo tu mismo en
# esa sesion con:  %pip install oracledb>=2.0.0  y luego dbutils.library.restartPython()
# No vuelvas a dejar eso fijo en el archivo versionado.
try:
    import vera_framework  # noqa: F401
except ModuleNotFoundError:
    import sys, os
    notebook_path = (dbutils.notebook.entry_point.getDbutils()
                     .notebook().getContext().notebookPath().get())
    ruta_src = os.path.abspath(
        os.path.join("/Workspace", os.path.dirname(notebook_path).lstrip("/"), "..", "src"))
    if ruta_src not in sys.path:
        sys.path.insert(0, ruta_src)
    print(f"[dev interactivo] paquete no instalado; src agregado al path: {ruta_src}")

# COMMAND ----------
from vera_framework.config import FrameworkConfig
from vera_framework.orchestrator import MetadataOrchestrator
import uuid, json

# COMMAND ----------
try:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    run_id = f"{ctx.jobId().get()}_{ctx.runId().get()}"
except Exception:
    run_id = f"manual_{uuid.uuid4().hex[:8]}"
print(f"run_id = {run_id}")

# COMMAND ----------
config = FrameworkConfig.from_job_params(dbutils, run_id=run_id)
orchestrator = MetadataOrchestrator(spark, config, dbutils)
resumen = orchestrator.run_daily()

# COMMAND ----------
print(json.dumps(resumen, indent=2, default=str))
if resumen["fallidas"] > 0:
    raise RuntimeError(
        f"{resumen['fallidas']} de {resumen['total']} cargas fallaron")
