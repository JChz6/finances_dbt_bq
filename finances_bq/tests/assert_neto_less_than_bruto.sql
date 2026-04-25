SELECT
    *
FROM {{ ref('agg_pago_por_hora') }}
WHERE neto_mensual > bruto_mensual