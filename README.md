# vera_framework

Bundle de Databricks (Databricks Asset Bundle) con un **framework de ingestión metadata-driven** para EPM. Extrae datos desde **Oracle** (vía el driver **JDBC** `ojdbc` con `JayDeBeApi`) y los materializa como tablas **Delta** en Unity Catalog, orquestando las cargas a partir de una **tabla de control** en lugar de código hardcodeado.

## Cómo funciona
El job diario lee las cargas activas de la tabla de control, ejecuta la query asociada contra Oracle, persiste un Parquet intermedio en un Volume y hace `TRUNCATE + INSERT` en la tabla destino, registrando cada ejecución en una tabla de log.

## Estructura
- `src/vera_framework/` — paquete Python del framework (config, orquestador, extractor Oracle, loader Delta, control_manager, queries).
- `notebooks/` — entrypoint del job (`20_ejecutar_framework.py`) y utilitarios (creación de rutas de volume, seed de control, validación).
- `databricks.yml` — definición del bundle y los tres targets de despliegue.
- `pipeline/deploy-bundle.yml` — pipeline de Azure DevOps (deploy por rama → ambiente).

## Ambientes
Se despliega a tres targets, cada uno corriendo como su propio service principal:

| Target | Rama | Catálogo destino |
|---|---|---|
| `dllo` | desarrollo | `epm_datalabs_catalog_dllo` |
| `uat` | pruebas | `epm_datalake_catalog_np` |
| `pdn` | produccion | `epm_datalake_catalog_prod` |

## Notas operativas
- Las dependencias (`JayDeBeApi` + `JPype1`) se instalan **a nivel de notebook** con `%pip` (no a nivel de cluster, por restricción de plataforma).
- El service principal de cada ambiente requiere GRANTs de Unity Catalog (`USE CATALOG`/`USE SCHEMA`, `CREATE TABLE`, `SELECT`/`MODIFY` en las tablas y `READ`/`WRITE VOLUME`).

## Conexión a Oracle (backend único: JDBC)
Todos los ambientes (**dllo / uat / pdn**) se conectan con el driver **Oracle JDBC `ojdbc`** (Java puro) vía `JayDeBeApi` + `JPype1`, corriendo en el **driver** del cluster. Se usa el mismo backend en todos por **reproducibilidad y coherencia de despliegue entre ambientes** (el comportamiento no cambia por usar distinto cliente). Además, el driver JDBC autentica contra cuentas con verificador de contraseña **10G**, que `python-oracledb` (thin) no soporta (`DPY-3015`), sin necesidad de Oracle Instant Client.

- **Driver requerido:** `ojdbc11-23.26.2.0.0.jar` (compatible con JDK 17 del Databricks Runtime 16.4). Descarga oficial: Maven Central `com.oracle.database.jdbc:ojdbc11:23.26.2.0.0`.
- **Ruta del jar:** cada target define `oracle_jdbc_jar_path` apuntando al jar subido a su Volume. En `databricks.yml` están marcados como `PUNTO 1/3` (dllo), `PUNTO 2/3` (uat) y `PUNTO 3/3` (pdn). ⚠️ Las rutas son **tentativas** (`TODO(plataforma)`): confirmar la ruta definitiva del jar antes de desplegar.