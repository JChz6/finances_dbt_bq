--TESTING: dias trabajados no puede ser mayor que 31

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "txn_time",
            "data_type": "datetime",
            "granularity": "month"
        }
    )
}}


WITH salario_mensual AS (
  SELECT
    txn_time,
    EXTRACT(YEAR FROM txn_time) as anio,
    EXTRACT(MONTH FROM txn_time) as mes,
    categoria,
    concepto,
    importe_nativo,
    SUM(
      CASE
        WHEN categoria = 'Salario' THEN importe_nativo
        ELSE 0
      END
    ) OVER(PARTITION BY EXTRACT(YEAR FROM txn_time), EXTRACT(MONTH FROM txn_time), moneda) as bruto_mensual,
    SUM(
      CASE
        WHEN categoria != 'Salario' THEN importe_nativo
        ELSE 0
      END
    ) OVER(PARTITION BY EXTRACT(YEAR FROM txn_time), EXTRACT(MONTH FROM txn_time), moneda) as descuentos,
    moneda,
    SUM(dias_trabajados) OVER(PARTITION BY EXTRACT(YEAR FROM txn_time), EXTRACT(MONTH FROM txn_time)) as dias_trabajados_mes,
    dias_trabajados,
    fecha_carga
  FROM {{ref('fact_transactions')}}
  WHERE categoria = 'Salario'
  OR concepto IN ('Aporte AFP', 'Seguro AFP', 'Quinta categoría')

  {% if is_incremental() %}
    AND
    (
      fecha_carga > (SELECT MAX(DATETIME_TRUNC(fecha_carga, MONTH)) from {{ this }})
    )
  {% endif %}

  )

  SELECT
    txn_time,
    anio,
    mes,
    dias_trabajados,
    dias_trabajados_mes,
    ROUND(dias_trabajados * 0.723, 2) as dias_trabajados_neto,
    ROUND(dias_trabajados * 0.723, 2) * 8 as horas_trabajadas,
    bruto_mensual,
    descuentos,
    bruto_mensual - descuentos as neto_mensual,
    ROUND((bruto_mensual - descuentos) / ((ROUND(dias_trabajados * 0.723, 2)) * 8),2) as pago_por_hora,
    moneda,
    fecha_carga 
  FROM salario_mensual
  QUALIFY ROW_NUMBER() OVER(PARTITION BY anio, mes, moneda ORDER BY dias_trabajados DESC, txn_time DESC) = 1