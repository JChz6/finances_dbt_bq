{{
    config(
        materialized='table'
    )
}}

-- Fuente única de "gasto esencial de supervivencia" para /crecimiento-kpis
-- (supervivencia_estricta), /gasto-esencial-discrecional (esencial) y /libertad-financiera
-- (indispensable) — antes cada endpoint hardcodeaba su propia lista de categorías en Python
-- y las tres divergían (ver dashboard_api/docs/metricas.md, sección 0.3).
--
-- gasto_esencial_real / gasto_discrecional_real: sobre fact_transactions (lo que
-- efectivamente se gastó). Excluye 'Inversiones'/'Deudas' (construcción de patrimonio, no
-- consumo — ver tasa_construccion_patrimonio) y 'Anuncios'/'Préstamos' brutos (se usan
-- netos, ver agg_netos_prestamos_anuncios) del bucket discrecional.
-- presupuesto_esencial: sobre presupuesto_materialized (lo planeado), para
-- supervivencia_estricta.
-- Ambos suman el componente real de interés+seguros de la hipoteca cuando el inmueble es
-- vivienda propia (hipotecas_materialized.costo_vida_interes_seguros, ver punto 1) — es el
-- reemplazo directo de 'Deudas'/'Deudas indispensables', que nunca capturó la hipoteca.

-- hipotecas_materialized trae el cronograma completo (cuotas futuras hasta el fin del
-- préstamo, ~2040) — sin este filtro, todos_los_meses más abajo se llena de meses futuros
-- vacíos (0 en todo) que no aportan nada y ensucian la serie de /gasto-esencial-discrecional.
WITH hipoteca_mensual AS (
    SELECT
        DATE_TRUNC(fecha_vencimiento, MONTH) AS mes,
        SUM(IF(pagado, costo_vida_interes_seguros, 0)) AS hipoteca_interes_seguros_vida_propia
    FROM {{ ref('hipotecas_materialized') }}
    WHERE fecha_vencimiento <= CURRENT_DATE()
    GROUP BY 1
),

real_mensual AS (
    SELECT
        DATE_TRUNC(DATE(txn_time), MONTH) AS mes,
        SUM(IF(UPPER(TRIM(categoria)) IN UNNEST({{ categorias_esenciales_supervivencia() }}), importe_moneda_principal, 0)) AS gasto_esencial_categorias,
        SUM(
            IF(
                UPPER(TRIM(categoria)) NOT IN UNNEST({{ categorias_esenciales_supervivencia() }})
                AND categoria NOT IN ('Inversiones', 'Deudas', 'Anuncios', 'Préstamos'),
                importe_moneda_principal, 0
            )
        ) AS gasto_discrecional_bruto
    FROM {{ ref('fact_transactions') }}
    WHERE ingreso_gasto = 'Gastos'
    GROUP BY 1
),

presupuesto_mensual AS (
    SELECT
        DATE_TRUNC(fecha, MONTH) AS mes,
        SUM(IF(UPPER(TRIM(categoria)) IN UNNEST({{ categorias_esenciales_supervivencia() }}), presupuesto, 0)) AS presupuesto_esencial_categorias
    FROM {{ ref('presupuesto_materialized') }}
    GROUP BY 1
),

netos AS (
    SELECT mes, neto_prestamos_terceros, neto_anuncios
    FROM {{ ref('agg_netos_prestamos_anuncios') }}
),

todos_los_meses AS (
    SELECT mes FROM real_mensual
    UNION DISTINCT SELECT mes FROM presupuesto_mensual
    UNION DISTINCT SELECT mes FROM hipoteca_mensual
    UNION DISTINCT SELECT mes FROM netos
)

SELECT
    m.mes,
    COALESCE(r.gasto_esencial_categorias, 0) + COALESCE(h.hipoteca_interes_seguros_vida_propia, 0) AS gasto_esencial_real,
    COALESCE(r.gasto_discrecional_bruto, 0) + COALESCE(n.neto_prestamos_terceros, 0) + COALESCE(n.neto_anuncios, 0) AS gasto_discrecional_real,
    COALESCE(p.presupuesto_esencial_categorias, 0) + COALESCE(h.hipoteca_interes_seguros_vida_propia, 0) AS presupuesto_esencial,
    COALESCE(n.neto_prestamos_terceros, 0) AS neto_prestamos_terceros,
    COALESCE(n.neto_anuncios, 0) AS neto_anuncios
FROM todos_los_meses m
LEFT JOIN real_mensual r ON m.mes = r.mes
LEFT JOIN presupuesto_mensual p ON m.mes = p.mes
LEFT JOIN hipoteca_mensual h ON m.mes = h.mes
LEFT JOIN netos n ON m.mes = n.mes
ORDER BY m.mes
