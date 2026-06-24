{{
    config(
        materialized='view'
    )
}}

WITH ingresos_mensuales AS (
    SELECT
      DATE_TRUNC(txn_time, MONTH) AS fecha,
      EXTRACT(YEAR FROM txn_time) AS anio,
      EXTRACT(MONTH FROM txn_time) AS mes,
      SUM(
        CASE
          WHEN categoria IN ('Salario', 'Freelance', 'Pasivo') THEN importe_moneda_principal
          ELSE 0
        END
        ) AS ingreso_bruto_soles,
      
      SUM(
        CASE
          WHEN categoria IN ('Jubilación', 'Comisiones') OR concepto IN ('Quinta categoría', 'ITF') THEN importe_moneda_principal
          ELSE 0
        END
        ) AS descuentos_soles
    
    FROM {{ ref('fact_transactions') }}
    GROUP BY ALL
  )

  SELECT
    fecha,
    anio,
    mes,
    ingreso_bruto_soles,
    descuentos_soles,
    ingreso_bruto_soles - descuentos_soles AS ingreso_neto,
    ROUND(PERCENTILE_CONT((ingreso_bruto_soles - descuentos_soles), 0.5) OVER(PARTITION BY anio), 2) AS ingreso_neto_promedio
  FROM ingresos_mensuales
  ORDER BY anio desc , mes desc