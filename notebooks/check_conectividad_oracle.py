# Databricks notebook source
# MAGIC %md # 00b - conectividad a Oracle
# MAGIC
# MAGIC Verifica, ANTES de correr el framework, que el cluster puede llegar a Oracle.


# COMMAND ----------
# ───── Pasos 1 y 2: RED (DNS + TCP). Sin dependencias externas. ─────
import socket

dbutils.widgets.text("oracle_host", "epm-to34.corp.epm.com.co")
dbutils.widgets.text("oracle_port", "1521")
dbutils.widgets.text("oracle_service", "SFUAT")
dbutils.widgets.text("oracle_secret_scope", "AZ-SecretScopeDBKS-EPM-NP-KV-UAT")
dbutils.widgets.text("oracle_user", "SQL_EPMBOTPD05")
dbutils.widgets.text("oracle_password_key", "AZ-SECRET-EPM-BOTPD05-FACTURACION-CTATECNICA")

HOST = dbutils.widgets.get("oracle_host")
PORT = int(dbutils.widgets.get("oracle_port"))
print(f"Objetivo de red: {HOST}:{PORT}")

# Paso 1: DNS
try:
    ip = socket.gethostbyname(HOST)
    print(f"OK  DNS: {HOST} -> {ip}")
except Exception as e:
    raise RuntimeError(
        f"DNS FALLA para {HOST}: {e}\n"
        f"El workspace no resuelve el dominio corporativo. Falta DNS privado en "
        f"la VNet del workspace, o usa la IP directa en el widget oracle_host."
    )

# Paso 2: TCP
try:
    s = socket.create_connection((HOST, PORT), timeout=10)
    s.close()
    print(f"OK  TCP: {HOST}:{PORT} alcanzable")
except Exception as e:
    raise RuntimeError(
        f"TCP FALLA a {HOST}:{PORT}: {e}\n"
        f"-> Problema de RED (firewall/ruta). El subnet del cluster no alcanza Oracle.\n"
        f"   (a) abrir salida del subnet del workspace hacia "
        f"{HOST}:{PORT}, (b) validar peering/route de la VNet de Databricks hacia "
        f"la red on-prem de Oracle."
    )

# COMMAND ----------
# Instala oracledb (solo para el Paso 3)
%pip install oracledb>=2.0.0
dbutils.library.restartPython()

# COMMAND ----------
# Paso 3: Oracle (handshake + credenciales + listener)
import oracledb

HOST = dbutils.widgets.get("oracle_host")
PORT = int(dbutils.widgets.get("oracle_port"))
SERVICE = dbutils.widgets.get("oracle_service")
SCOPE = dbutils.widgets.get("oracle_secret_scope")
USER = dbutils.widgets.get("oracle_user")
PWD_KEY = dbutils.widgets.get("oracle_password_key")

dsn = f"{HOST}:{PORT}/{SERVICE}"
print(f"Probando: {dsn}  (user={USER}, scope={SCOPE})")

pwd = dbutils.secrets.get(scope=SCOPE, key=PWD_KEY)
try:
    conn = oracledb.connect(user=USER, password=pwd, dsn=dsn)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM DUAL")
    ok = cur.fetchone()[0]
    cur.close()
    conn.close()
    print(f"OK  Oracle responde (SELECT 1 FROM DUAL = {ok}). Red + credenciales OK.")
except oracledb.DatabaseError as e:
    raise RuntimeError(
        f"La RED llego a Oracle pero la conexion fue rechazada: {e}\n"
        f"-> ORA-01017 = usuario/clave (revisa oracle_user y el secreto {SCOPE}/{PWD_KEY}).\n"
        f"-> ORA-12514 = el service '{SERVICE}' no esta registrado en el listener.\n"
        f"   DSN usado: {dsn}"
    )

# COMMAND ----------
print("\n=== PREFLIGHT OK: el cluster puede usar Oracle ===")
