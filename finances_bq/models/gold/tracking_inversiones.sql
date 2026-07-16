{{
    config(
        materialized='view'
    )
}}

WITH relevantes AS(
  SELECT
    txn_time, month_id, categoria, concepto, importe_nativo, moneda, importe_moneda_principal,
    CASE 
      WHEN categoria IN ('Salario', 'Freelance', 'Pasivo') THEN TRUE
      ELSE FALSE
    END AS flg_ingreso,
    CASE 
      WHEN (cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB') AND ingreso_gasto = 'Dinero ingresado') THEN TRUE
      WHEN (categoria = 'Inversiones' AND subcategoria = 'FIBRAS') THEN TRUE
      WHEN (categoria = 'Inversiones' AND subcategoria = 'Inmuebles' AND concepto = 'Amortización') THEN TRUE
      ELSE FALSE
    END AS flg_inversion,
    CASE 
      WHEN (cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB') AND ingreso_gasto = 'Dinero ingresado') THEN 'Cuentas Alto Rendimiento'
      WHEN (categoria = 'Inversiones' AND subcategoria = 'FIBRAS') THEN 'FIBRAS'
      WHEN (categoria = 'Inversiones' AND subcategoria = 'Inmuebles' AND concepto = 'Amortización') THEN 'Prepagos'
      ELSE NULL
    END AS tipo_inversion,

    CASE WHEN (cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB') AND ingreso_gasto = 'Dinero ingresado') THEN importe_moneda_principal ELSE 0 END AS invertido_cuentas_alto_rendimiento,
    CASE WHEN (categoria = 'Inversiones' AND subcategoria = 'FIBRAS') THEN importe_moneda_principal ELSE 0 END AS invertido_fibras,
    CASE WHEN (categoria = 'Inversiones' AND subcategoria = 'Inmuebles' AND concepto = 'Amortización') THEN importe_moneda_principal ELSE 0 END AS invertido_prepagos
  FROM {{ ref('fact_transactions') }}
  WHERE categoria IN ('Salario', 'Freelance', 'Pasivo')
  --cuentas de alto rendimiento
  OR (cuenta IN ('Wow Compartamos', 'Pichincha', 'GNB') AND ingreso_gasto = 'Dinero ingresado')
  --inversión en FIBRAS
  OR (categoria = 'Inversiones' AND subcategoria = 'FIBRAS')
  --amortizaciones a capital
  OR (categoria = 'Inversiones' AND subcategoria = 'Inmuebles' AND concepto = 'Amortización') 
),
acumulados AS(
  SELECT
    DATE(txn_time) AS fecha,
    r.month_id,
    r.categoria,
    r.concepto,
    r.importe_moneda_principal,
    -- Rolling SUM de ingreso bruto en el mes
    SUM(IF(flg_ingreso, importe_moneda_principal, 0)) OVER(
      PARTITION BY  r.month_id ORDER BY txn_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS bruto_mensual_acumulado,
    -- Join con asignación mensual a CAR
    pi.cuenta_alto_rendimiento AS cuentas_alto_rendimiento_asignado,  
    -- ROlling SUM de inversiones mensuales en CAR
    SUM(invertido_cuentas_alto_rendimiento) OVER(
      PARTITION BY  r.month_id ORDER BY txn_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cuentas_alto_rendimiento_inversion_acumulada,

    -- JOin con asignación a FIBPRIME
    pi.fibprime AS fibprime_asignado, 
    -- Rolling SUM de inversion mensual en FIBPRIME
    SUM(IF(concepto = 'FIBPRIME', importe_moneda_principal, 0)) OVER(
      PARTITION BY  r.month_id ORDER BY txn_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS fibprime_inversion_acumulada,

    -- JOin, destinado mensual a prepagos
    pi.amortizacion_hipoteca AS prepagos_asignado,
    -- Rolling SUM de inversiones en prepagos
    SUM(invertido_prepagos) OVER(
      PARTITION BY  r.month_id ORDER BY txn_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prepagos_inversion_acumulada
  FROM relevantes r
  LEFT JOIN {{ ref('plan_inversion') }} pi
    ON DATETIME_TRUNC(r.txn_time, MONTH) = pi.fecha
)
SELECT
  fecha,
  month_id,
  --categoria,
  --concepto,
  importe_moneda_principal,
  bruto_mensual_acumulado,
  cuentas_alto_rendimiento_asignado,
  cuentas_alto_rendimiento_inversion_acumulada,
  
  fibprime_asignado,
  fibprime_inversion_acumulada,
  
  prepagos_asignado,
  prepagos_inversion_acumulada,
  cuentas_alto_rendimiento_asignado - cuentas_alto_rendimiento_inversion_acumulada AS cuentas_alto_rendimiento_restante,
  
  fibprime_asignado - fibprime_inversion_acumulada AS fibprime_restante,
  
  prepagos_asignado - prepagos_inversion_acumulada AS prepagos_restante
FROM acumulados
QUALIFY ROW_NUMBER() OVER(PARTITION BY month_id ORDER BY fecha DESC) = 1
ORDER BY fecha
