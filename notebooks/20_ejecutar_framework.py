# Databricks notebook source
# MAGIC %md # Entrypoint del framework metadata-driven (job diario)

# COMMAND ---------- 
# MAGIC %pip install oracledb>=2.0.0 
# COMMAND ---------- 
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("ambiente", "dev")
dbutils.widgets.text("catalog_destino", "epm_datalabs_catalog_dllo")
dbutils.widgets.text("schema_destino", "facturacion")
dbutils.widgets.text("volume_path",
    "/Volumes/epm_datalake_vol_np/facturacion_vol/facturacion_bronze_vol/midas_framework/dev")
dbutils.widgets.text("oracle_secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-DLLO")
dbutils.widgets.text("oracle_user", "SQL_EPMBOTPD05")
dbutils.widgets.text("oracle_password_key", "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA")
dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co")
dbutils.widgets.text("oracle_port", "1521")
dbutils.widgets.text("oracle_service", "SFUAT")

# COMMAND ---------- 
import sys, os 
# Ruta a la carpeta src/ del bundle, relativa a este notebook. 
# # Este notebook está en notebooks/, el paquete está en ../src/ 
notebook_dir = os.path.dirname(     
    dbutils.notebook.entry_point.getDbutils().notebook()
    .getContext().notebookPath().get() 
) 
ruta_src = os.path.abspath(os.path.join("/Workspace", notebook_dir.lstrip("/"), "..", "src")) 

if ruta_src not in sys.path:
    sys.path.insert(0, ruta_src) 
    
print(f"src agregado al path: {ruta_src}")

# COMMAND ----------
from midas_framework.config import FrameworkConfig
from midas_framework.orchestrator import MetadataOrchestrator
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
