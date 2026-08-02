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
        ) AS gasto_acumulado_mes,
        p.presupuesto,
        MAX(h.fecha_carga) OVER(PARTITION BY DATE_TRUNC(h.txn_time, MONTH)) AS fecha_carga
    FROM base_fact h
    FULL OUTER JOIN {{source('finances_raw', 'presupuesto')}} p
    ON UPPER(h.categoria) = UPPER(p.categoria)
    AND
    DATE_TRUNC(p.fecha, MONTH) = DATE_TRUNC(h.txn_time, MONTH)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY COALESCE(DATE_TRUNC(h.txn_time, MONTH), DATE_TRUNC(p.fecha, MONTH)), categoria) = 1
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
FROM first_layer