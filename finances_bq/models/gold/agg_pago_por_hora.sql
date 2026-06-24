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
    EXTRACT(YEAR FROM txn_time) AS anio,
    EXTRACT(MONTH FROM txn_time) AS mes,
    MAX(txn_time) AS ultima_txn_time,
    MAX(fecha_carga) AS ultima_fecha_carga,
    
    -- Sumamos el ingreso bruto de todos los trabajos del mes (en la moneda principal)
    SUM(CASE WHEN categoria = 'Salario' THEN importe_moneda_principal ELSE 0 END) AS bruto_mensual,
    
    -- Sumamos todos los descuentos (en la moneda principal)
    SUM(CASE WHEN categoria != 'Salario' THEN importe_moneda_principal ELSE 0 END) AS descuentos,
    
    -- Sumamos los días trabajados de todas las fuentes de ingresos (salarios) en el mes
    SUM(CASE WHEN categoria = 'Salario' THEN dias_trabajados ELSE 0 END) AS total_dias_trabajados
    
  FROM {{ref('fact_transactions')}}
  WHERE (categoria = 'Salario'
     OR concepto IN ('Aporte AFP', 'Seguro AFP', 'Quinta categoría')
     OR concepto LIKE 'Comisión %')

  {% if is_incremental() %}
    AND fecha_carga > (SELECT MAX(DATETIME_TRUNC(fecha_carga, MONTH)) FROM {{ this }})
  {% endif %}
  
  -- Agrupamos únicamente por año y mes para consolidar múltiples trabajos
  GROUP BY 1, 2
)

SELECT
  ultima_txn_time AS txn_time,
  anio,
  mes,
  total_dias_trabajados AS dias_trabajados_mes,
  
  -- Cálculos de tiempo
  ROUND(total_dias_trabajados * 0.723, 2) AS dias_trabajados_neto,
  ROUND(total_dias_trabajados * 0.723, 2) * 8 AS horas_trabajadas,
  
  -- Cálculos monetarios
  bruto_mensual,
  descuentos,
  bruto_mensual - descuentos AS neto_mensual,
  
  -- Valor de tu Hora de Vida
  -- Usamos NULLIF para evitar errores de división por cero si algún mes no tuvo días trabajados
  ROUND(
    (bruto_mensual - descuentos) / NULLIF((ROUND(total_dias_trabajados * 0.723, 2) * 8), 0), 
  2) AS pago_por_hora,  
  'PEN' AS moneda, -- Consolidado en la moneda principal (PEN)
  ultima_fecha_carga AS fecha_carga 
FROM salario_mensual