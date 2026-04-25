{{
    config(
        materialized='incremental',
        on_schema_change = 'append_new_columns',
        incremental_strategy='merge',
        unique_key = 'txn_id',
        partition_by={
            "field": "txn_time",
            "data_type": "datetime",
            "granularity": "month"
        },
        pre_hook = "{{ replace_partitions(ref('fact_transactions'), this, 'txn_time', 'txn_time') }}"
    )
}}


WITH ultimo_pago AS (
    SELECT
      horas_trabajadas,
      pago_por_hora
    FROM
      {{ref('agg_pago_por_hora')}}
    ORDER BY
      anio DESC, mes DESC
    LIMIT 1
  )
  SELECT
    h.txn_time,
    h.txn_id,
    EXTRACT(YEAR FROM h.txn_time) as anio,
    EXTRACT(MONTH FROM h.txn_time) as mes,
    h.cuenta,
    h.categoria,
    h.subcategoria,
    h.concepto,
    h.comentario,
    h.importe_moneda_principal,
    COALESCE(v.horas_trabajadas, u.horas_trabajadas) AS horas_trabajadas,
    COALESCE(v.pago_por_hora, u.pago_por_hora) AS pago_por_hora,
    ROUND(SAFE_DIVIDE(h.importe_moneda_principal, COALESCE(v.pago_por_hora, u.pago_por_hora)), 3) AS costo_en_horas,
    h.fecha_carga
  FROM
    {{ref('fact_transactions')}} h
  LEFT JOIN
    {{ref('agg_pago_por_hora')}} v
  ON
    EXTRACT(YEAR FROM h.txn_time) = v.anio AND EXTRACT(MONTH FROM h.txn_time) = v.mes
  CROSS JOIN
    ultimo_pago u
  WHERE
    h.ingreso_gasto = 'Gastos'

{% if is_incremental() %}
    AND h.fecha_carga > (SELECT MAX(fecha_carga) FROM {{ this }}) 
{% endif %}