{{
    config(
        materialized='view'
    )
}}

WITH cuota_hipoteca AS(
  SELECT 
    DATE_TRUNC(fecha_vencimiento, MONTH) AS fecha,
    CAST(FORMAT_DATE('%Y%m', fecha_vencimiento) AS INT64) AS month_id,
    cuota_mensual
  FROM {{ ref('hipotecas_materialized') }}
  WHERE DATE_TRUNC(fecha_vencimiento, MONTH) BETWEEN '2025-10-15' AND CURRENT_DATE()
),
gasto_proyectado AS(
  SELECT
    fecha,
    SUM(
      CASE
        WHEN categoria NOT IN ('Deudas', 'Deudas indispensables') THEN presupuesto
        ELSE 0
      END
    )* 1.18 AS gasto_proyectado 
 FROM {{ ref('presupuesto_materialized') }}
 GROUP BY fecha
),
precalculo AS(
SELECT
    DATETIME_TRUNC(txn_time, MONTH) AS fecha,
    month_id,
    -- 1. Bruto Mensual (Suma total de ingresos en el mes)
    SUM(CASE
        WHEN categoria IN ('Salario', 'Freelance', 'Pasivo') THEN importe_moneda_principal
        ELSE 0
    END) AS bruto_mensual,

    -- 2. Neto Mensual (Resta automáticamente si pertenece a esas categorías)
    SUM(CASE
        WHEN categoria IN ('Salario', 'Freelance', 'Pasivo') THEN importe_moneda_principal
        WHEN categoria IN ('Impuestos', 'Jubilación', 'Comisiones') THEN importe_moneda_principal * -1
        ELSE 0
    END) AS neto_mensual_estimado

FROM {{ ref('fact_transactions') }}
  WHERE
  categoria IN ('Salario', 'Freelance', 'Pasivo', 'Impuestos', 'Jubilación', 'Comisiones')
GROUP BY ALL
),
calculo_de_disponible AS (
  SELECT 
    pc.fecha,
    pc.month_id,
    pc.bruto_mensual,
    pc.neto_mensual_estimado,
    -- 3. Sobre el neto, resta los gastos de vida (fijado en 3K)
    (neto_mensual_estimado - gp.gasto_proyectado) AS neto_despues_de_vida,
    cuota_mensual,
      -- 4. A eso, le resta la cuota de la hipoteca correspondiente a ese mes
    (neto_mensual_estimado - gp.gasto_proyectado) - cuota_mensual AS disponible_ahorro_inversion,
  FROM precalculo pc
  LEFT JOIN cuota_hipoteca ch USING(fecha)
  LEFT JOIN gasto_proyectado gp USING(fecha)
  )
SELECT
  CAST(fecha AS DATE) AS fecha,
  month_id,
  bruto_mensual,
  neto_mensual_estimado,
  neto_despues_de_vida,
  cuota_mensual,
  disponible_ahorro_inversion,
  FLOOR(disponible_ahorro_inversion * 0.27) AS cuenta_alto_rendimiento,
  FLOOR(disponible_ahorro_inversion * 0.1) AS amortizacion_hipoteca,
  FLOOR(disponible_ahorro_inversion * 0.63) AS fibras,
  FLOOR(disponible_ahorro_inversion * 0.63 * 0.7) AS fibprime,
  FLOOR(disponible_ahorro_inversion * 0.63 * 0.3) AS fibccap,
FROM calculo_de_disponible

