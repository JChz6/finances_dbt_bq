{{
    config(
        materialized='table'
    )
}}

-- Netea, por mes, dos categorías que en fact_transactions viven mezcladas con su
-- contraparte de 'Reembolsos' y por eso inflan el gasto discrecional bruto si se cuentan
-- completas (ver dashboard_api/docs/metricas.md):
--   - Préstamos: micro-adelantos a conocidos que se identifican por el patrón de comentario
--     'C/<Nombre>' (mismo patrón en la fila de Reembolsos que los devuelve). No se intenta
--     matching exacto por transacción, solo un neteo agregado por persona/mes: lo que no
--     tiene contraparte de Reembolsos con el mismo nombre en el mismo mes queda como pérdida
--     real (préstamos a familia, o adelantos que aún no se han devuelto).
--   - Anuncios: publicidad que se paga y luego se reembolsa (concepto 'Ads%' bajo
--     'Reembolsos'). Es esperable un neto negativo pequeño histórico (fuga ya corregida).

WITH base_prestamos_reembolsos AS (
    SELECT
        DATE_TRUNC(DATE(txn_time), MONTH) AS mes,
        categoria,
        importe_moneda_principal,
        -- primer token después de 'C/', ignorando sufijos tipo 'C/ Daphne - Cold Brew'
        REGEXP_EXTRACT(TRIM(comentario), r'^C/\s*([^\s-]+)') AS persona
    FROM {{ ref('fact_transactions') }}
    WHERE categoria IN ('Préstamos', 'Reembolsos')
),

prestamos_con_persona AS (
    SELECT
        mes,
        persona,
        SUM(IF(categoria = 'Préstamos', importe_moneda_principal, 0)) AS prestamos,
        SUM(IF(categoria = 'Reembolsos', importe_moneda_principal, 0)) AS reembolsos
    FROM base_prestamos_reembolsos
    WHERE persona IS NOT NULL
    GROUP BY mes, persona
),

prestamos_netos_mensual AS (
    SELECT mes, SUM(prestamos - reembolsos) AS neto_con_persona
    FROM prestamos_con_persona
    GROUP BY mes
),

-- Préstamos sin comentario 'C/<Nombre>' identificable: no se netean contra nada (no hay
-- forma de saber si tienen contraparte), cuentan completos como gasto/pérdida del mes.
prestamos_sin_persona AS (
    SELECT mes, SUM(importe_moneda_principal) AS prestamos_sin_neteo
    FROM base_prestamos_reembolsos
    WHERE persona IS NULL AND categoria = 'Préstamos'
    GROUP BY mes
),

anuncios_mensual AS (
    SELECT
        DATE_TRUNC(DATE(txn_time), MONTH) AS mes,
        SUM(importe_moneda_principal) AS anuncios
    FROM {{ ref('fact_transactions') }}
    WHERE categoria = 'Anuncios'
    GROUP BY mes
),

reembolsos_ads_mensual AS (
    SELECT
        DATE_TRUNC(DATE(txn_time), MONTH) AS mes,
        SUM(importe_moneda_principal) AS reembolsos_ads
    FROM {{ ref('fact_transactions') }}
    WHERE categoria = 'Reembolsos' AND concepto LIKE 'Ads%'
    GROUP BY mes
),

todos_los_meses AS (
    SELECT mes FROM prestamos_netos_mensual
    UNION DISTINCT SELECT mes FROM prestamos_sin_persona
    UNION DISTINCT SELECT mes FROM anuncios_mensual
    UNION DISTINCT SELECT mes FROM reembolsos_ads_mensual
),

netos AS (
    SELECT
        m.mes,
        COALESCE(pn.neto_con_persona, 0) + COALESCE(ps.prestamos_sin_neteo, 0) AS neto_prestamos_terceros,
        COALESCE(am.anuncios, 0) - COALESCE(ra.reembolsos_ads, 0) AS neto_anuncios
    FROM todos_los_meses m
    LEFT JOIN prestamos_netos_mensual pn ON m.mes = pn.mes
    LEFT JOIN prestamos_sin_persona ps ON m.mes = ps.mes
    LEFT JOIN anuncios_mensual am ON m.mes = am.mes
    LEFT JOIN reembolsos_ads_mensual ra ON m.mes = ra.mes
),

-- Mismo patrón que agg_costo_en_vida: pago por hora del mes de la transacción, o el más
-- reciente conocido si el mes no tiene uno propio (útil para el mes en curso).
ultimo_pago AS (
    SELECT horas_trabajadas, pago_por_hora
    FROM {{ ref('agg_pago_por_hora') }}
    ORDER BY anio DESC, mes DESC
    LIMIT 1
)

SELECT
    n.mes,
    n.neto_prestamos_terceros,
    n.neto_anuncios,
    ROUND(SAFE_DIVIDE(n.neto_prestamos_terceros, COALESCE(v.pago_por_hora, u.pago_por_hora)), 3) AS neto_prestamos_terceros_horas,
    ROUND(SAFE_DIVIDE(n.neto_anuncios, COALESCE(v.pago_por_hora, u.pago_por_hora)), 3) AS neto_anuncios_horas
FROM netos n
LEFT JOIN {{ ref('agg_pago_por_hora') }} v
    ON EXTRACT(YEAR FROM n.mes) = v.anio AND EXTRACT(MONTH FROM n.mes) = v.mes
CROSS JOIN ultimo_pago u
ORDER BY n.mes
