"""
Registro de queries del framework. La tabla midas_control_cargas
referencia cada query por su clave (query_key).

Mantener el SQL aquí (y no en una celda Delta) permite:
  - versionado en Git
  - revisión de código de cambios al SQL
  - testing
"""
from __future__ import annotations

# El SQL completo de cada query está en la Sección 5.1 / 5.2 de la guía.
QUERIES: dict[str, str] = {
    "q1_promedio_subcategoria": """
        WITH DatoIni AS (
    SELECT sesunuse, sesuserv, sesucate, sesusuca, sesucicl,
           ORCRPECO, ORCRTICO
    FROM   or_order o, or_order_activity oa, cm_ordecrit critica, servsusc
    WHERE  o.ORDER_STATUS_ID = 5
      AND  o.order_id = oa.order_id
      AND  oa.activity_id = 102010
      AND  orcrsesu = oa.product_id
      AND  orcracti = oa.order_activity_id
      AND  sesunuse = oa.product_id
),
AnnoMes AS (
    SELECT
      CASE
        WHEN TO_CHAR(LAST_DAY(PECSFECI) - TO_CHAR(PECSFECI,'DD'),'DD') > TO_CHAR(PECSFECF,'DD')
          THEN TO_CHAR(PECSFECI,'MM')
        WHEN TO_CHAR(LAST_DAY(PECSFECI) - TO_CHAR(PECSFECI,'DD'),'DD') < TO_CHAR(PECSFECF,'DD')
          THEN TO_CHAR(PECSFECF,'MM')
        ELSE TO_CHAR(PECSFECI,'MM')
      END AS mes,
      CASE
        WHEN TO_CHAR(LAST_DAY(PECSFECI) - TO_CHAR(PECSFECI,'DD'),'DD') > TO_CHAR(PECSFECF,'DD')
          THEN TO_CHAR(PECSFECI,'YYYY')
        WHEN TO_CHAR(LAST_DAY(PECSFECI) - TO_CHAR(PECSFECI,'DD'),'DD') < TO_CHAR(PECSFECF,'DD')
          THEN TO_CHAR(PECSFECF,'YYYY')
        ELSE TO_CHAR(PECSFECI,'YYYY')
      END AS anno,
      DatoIni.*
    FROM DatoIni
    JOIN pericose ON pecscons = ORCRPECO
),
AnnoMes2 AS (
    SELECT
      CASE WHEN mes = '01' THEN 11
           WHEN mes = '02' THEN 12
           ELSE TO_NUMBER(mes) - 2
      END AS MesR,
      CASE WHEN mes = '01' OR mes = '02' THEN TO_NUMBER(anno) - 1
           ELSE TO_NUMBER(anno)
      END AS AnnoR,
      AnnoMes.*
    FROM AnnoMes
),
localidad AS (
    SELECT AnnoMes2.*, GEOGRAP_LOCATION_ID, a.address_id
    FROM AnnoMes2
    JOIN pr_product p ON p.product_id = sesunuse
    JOIN ab_address a ON a.address_id = p.address_id
)
SELECT
    CAST(coprsuca.CPSCCOTO / coprsuca.CPSCProd AS NUMBER(20,6)) AS PROM,
    sesunuse  AS SESUNUSE,
    ORCRPECO  AS ORCRPECO,
    ORCRTICO  AS ORCRTICO
FROM localidad
JOIN coprsuca
  ON coprsuca.CPSCUBGE = localidad.GEOGRAP_LOCATION_ID
 AND coprsuca.CPSCCATE = localidad.sesucate
 AND coprsuca.CPSCSUCA = localidad.sesusuca
 AND coprsuca.CPSCTCON = ORCRTICO
 AND coprsuca.CPSCANCO = localidad.anno
 AND coprsuca.CPSCMECO = localidad.mes
    """,
    "q2_promedio_individual_6m": """
        WITH DatoIni AS (
    SELECT sesunuse, sesuserv, sesucate, sesusuca, sesucicl,
           ORCRPECO, ORCRTICO,
           (SELECT *
            FROM (SELECT pefapecs
                  FROM perifact
                  WHERE pefacicl = sesucicl
                    AND pefapecs < ORCRPECO
                  ORDER BY PEFAFIMO DESC)
            WHERE rownum = 1) periodoCons
    FROM   or_order o, or_order_activity oa, cm_ordecrit critica, servsusc
    WHERE  o.ORDER_STATUS_ID = 5
      AND  o.order_id = oa.order_id
      AND  oa.activity_id = 102010
      AND  orcrsesu = oa.product_id
      AND  orcracti = oa.order_activity_id
      AND  sesunuse = oa.product_id
)
SELECT
    HCPPCOPR  AS HCPPCOPR,
    sesunuse  AS SESUNUSE,
    HCPPPECO  AS HCPPPECO,
    HCPPTICO  AS HCPPTICO
FROM DatoIni, hicoprpm
WHERE HCPPSESU = sesunuse
  AND HCPPTICO = ORCRTICO
  AND hcpppeco = periodoCons
    """,
}


def get_query(query_key: str) -> str:
    """Devuelve el SQL de una query. Lanza KeyError si no existe."""
    if query_key not in QUERIES:
        raise KeyError(
            f"query_key '{query_key}' no registrada. "
            f"Disponibles: {list(QUERIES.keys())}"
        )
    return QUERIES[query_key].strip()