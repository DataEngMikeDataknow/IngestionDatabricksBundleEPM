# Databricks notebook source
# MAGIC %md # Entrypoint del framework metadata-driven (job diario)
# MAGIC
# MAGIC Las dependencias (driver `oracledb`) se instalan a nivel de NOTEBOOK con
# MAGIC `%pip` (primera celda), mecanismo unico para TODOS los ambientes (dllo,
# MAGIC uat, pdn): por restriccion de plataforma no se pueden instalar librerias
# MAGIC a nivel de cluster. El paquete propio `vera_framework` se resuelve via
# MAGIC `sys.path` (los archivos del bundle, incluido `src/`, se sincronizan al
# MAGIC workspace), no por wheel.

# COMMAND ----------
# Instalacion de dependencias a nivel de NOTEBOOK. Es el mecanismo unico para
# TODOS los ambientes (dllo, uat, pdn): por restriccion de plataforma no se
# pueden instalar librerias a nivel de cluster en ningun entorno.
# Va de PRIMERO porque dbutils.library.restartPython() reinicia el kernel y
# borra todo lo definido antes (los widgets sobreviven, el codigo Python no).
#   - oracledb: driver thin (dllo/uat).
#   - JayDeBeApi + JPype1: solo los usa pdn, para conectar por Oracle JDBC thin
#     (soporta el verifier 10G). Se instalan en los 3 por simplicidad; en
#     dllo/uat quedan sin usar (el import es perezoso en oracle_extractor.py).
%pip install oracledb>=2.0.0 JayDeBeApi JPype1
dbutils.library.restartPython()

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
# "" => python-oracledb thin (dllo/uat). Con ruta de jar ojdbc en Volume =>
# Oracle JDBC thin via JayDeBeApi (pdn, por el verifier 10G).
dbutils.widgets.text("oracle_jdbc_jar_path", "")

# COMMAND ----------
# Resolucion del paquete propio `vera_framework` SIN instalarlo como wheel en
# ningun ambiente: si no esta en el path, se agrega `src/` del bundle (que el
# deploy sincroniza al workspace, p.ej. /Workspace/.../files/src) al sys.path.
# oracledb ya quedo instalado por el %pip de la primera celda.
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
