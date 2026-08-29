{{
    config(
        materialized='incremental',
        on_schema_change = 'append_new_columns',
        incremental_strategy='merge',
        unique_key = 'month_cat_id',
        partition_by={
            "field": "fecha",
            "data_type": "datetime",
            "granularity": "month"
        },
        pre_hook = "{{ replace_partitions(ref('fact_transactions'), this, 'txn_time', 'fecha') }}"
    )
}}



WITH base_fact AS(
    SELECT
        txn_time,
        categoria,
        importe_moneda_principal,
        ingreso_gasto,
        fecha_carga
    FROM {{ref('fact_transactions')}}
    WHERE ingreso_gasto = "Gastos"
),

-- 'Deudas indispensables' (la cuota mensual obligatoria de la hipoteca) nunca matchea por
-- categoría contra fact_transactions: la cuota se categoriza intencionalmente como
-- 'Inversiones', no 'Deudas' (decisión ya tomada, ver hipotecas_materialized). Sin este CTE,
-- esa fila siempre mostraba gasto_acumulado_mes = 0 / 'CUMPLE' aunque la cuota sí se pague.
-- Mismo patrón que agg_gasto_esencial para separar la hipoteca del match genérico.
hipoteca_mensual AS (
    SELECT
        DATE_TRUNC(fecha_vencimiento, MONTH) AS mes,
        SUM(cuota_mensual) AS cuota_pagada_mes
    FROM {{ ref('hipotecas_materialized') }}
    WHERE pagado
    GROUP BY 1
),

first_layer AS(
    SELECT
        TO_HEX(MD5(CONCAT(
            CAST(COALESCE(DATE_TRUNC(h.txn_time, MONTH), DATE_TRUNC(p.fecha, MONTH)) AS STRING),
            COALESCE(h.categoria, p.categoria)
        ))) AS month_cat_id,
        COALESCE(
            DATE_TRUNC(h.txn_time, MONTH),
            DATE_TRUNC(p.fecha, MONTH)
        ) AS fecha,
        COALESCE(h.categoria, p.categoria) as categoria,
        COALESCE(
            SUM(h.importe_moneda_principal)
                OVER(PARTITION BY
                    COALESCE(DATE_TRUNC(h.txn_time, MONTH),
                    DATE_TRUNC(p.fecha, MONTH)),
                    COALESCE(h.categoria, p.categoria)
                ),
            0
        ) AS gasto_acumulado_mes_generico,
        p.presupuesto,
        MAX(h.fecha_carga) OVER(PARTITION BY DATE_TRUNC(h.txn_time, MONTH)) AS fecha_carga
    -- Lee de presupuesto_materialized (normaliza 'Seguro'->'Seguros', 'Gastos variables'->
    -- 'Gastos Variables'), no del Sheet crudo: contra el Sheet crudo, 'Seguro' nunca
    -- matcheaba con 'Seguros' de fact_transactions y esa fila también daba 0/'CUMPLE' falso.
    FROM base_fact h
    FULL OUTER JOIN {{ref('presupuesto_materialized')}} p
    ON UPPER(h.categoria) = UPPER(p.categoria)
    AND
    DATE_TRUNC(p.fecha, MONTH) = DATE_TRUNC(h.txn_time, MONTH)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY COALESCE(DATE_TRUNC(h.txn_time, MONTH), DATE_TRUNC(p.fecha, MONTH)), categoria) = 1
),

con_hipoteca AS (
    SELECT
        fl.month_cat_id,
        fl.fecha,
        fl.categoria,
        CASE
            WHEN fl.categoria = 'Deudas indispensables' THEN COALESCE(hm.cuota_pagada_mes, 0)
            ELSE fl.gasto_acumulado_mes_generico
        END AS gasto_acumulado_mes,
        fl.presupuesto,
        fl.fecha_carga
    FROM first_layer fl
    LEFT JOIN hipoteca_mensual hm ON fl.fecha = hm.mes
)
SELECT
    month_cat_id,
    fecha,
    categoria,
    gasto_acumulado_mes,
    presupuesto,
    CONCAT(
        ROUND(SAFE_DIVIDE(gasto_acumulado_mes, presupuesto)*100, 3),
        '%')
         AS utilizado,
    ROUND(presupuesto - gasto_acumulado_mes, 2) AS presupuesto_disponible,
    CASE
        WHEN presupuesto IS NOT NULL AND presupuesto >= gasto_acumulado_mes THEN 'CUMPLE'
        WHEN presupuesto IS NOT NULL AND presupuesto < gasto_acumulado_mes THEN 'EXCESO'
        ELSE NULL
    END AS cumplimiento,
    fecha_carga
FROM con_hipoteca