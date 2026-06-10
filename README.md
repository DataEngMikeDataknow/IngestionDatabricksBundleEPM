# vera_framework

Bundle de Databricks (Databricks Asset Bundle) con un **framework de ingestión metadata-driven** para EPM. Extrae datos desde **Oracle** (vía `python-oracledb`) y los materializa como tablas **Delta** en Unity Catalog, orquestando las cargas a partir de una **tabla de control** en lugar de código hardcodeado.

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
- Las dependencias (`oracledb`) se instalan **a nivel de notebook** con `%pip` (no a nivel de cluster, por restricción de plataforma).
- El service principal de cada ambiente requiere GRANTs de Unity Catalog (`USE CATALOG`/`USE SCHEMA`, `SELECT`/`MODIFY` en las tablas y `READ/WRITE VOLUME`).
