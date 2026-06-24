WITH CTE AS (
    SELECT
        *
    FROM {{source('finances_raw', 'hip_depa_alameda_dolores')}}
)

SELECT 
    CAST(inmueble_id AS STRING) AS inmueble_id,
    CAST(pagado AS BOOLEAN) AS pagado,
    CAST(fecha_vencimiento AS DATE) AS fecha_vencimiento,
    CAST(dias AS INT64) AS dias,
    CAST(num_pago AS INT64) AS num_pago,
    CAST(saldo_inicial AS NUMERIC) AS saldo_inicial,
    CAST(cuota_mensual AS  NUMERIC) AS cuota_mensual,
    CAST(seguro_propiedad AS  NUMERIC) AS seguro_propiedad,
    CAST(seguro_desgravamen AS  NUMERIC) AS seguro_desgravamen,
    CAST(porc_seguros AS  NUMERIC) AS porc_seguros,
    CAST(presupuesto_personal AS  NUMERIC) AS presupuesto_personal,
    CAST(ingreso_alquiler AS  NUMERIC) AS ingreso_alquiler,
    CAST(amortizacion_capital AS  NUMERIC) AS amortizacion_capital,
    CAST(robo_amortizacion AS  NUMERIC) AS robo_amortizacion,
    CAST(capital_cuota AS  NUMERIC) AS capital_cuota,
    CAST(porc_capital AS  NUMERIC) AS porc_capital,
    CAST(capital_total AS  NUMERIC) AS capital_total,
    CAST(interes_cuota AS  NUMERIC) AS interes_cuota,
    CAST(porc_interes AS  NUMERIC) AS porc_interes,
    CAST(total_pagado AS  NUMERIC) AS total_pagado,
    CAST(saldo_final AS  NUMERIC) AS saldo_final,
    CAST(nueva_cuota AS  NUMERIC) AS nueva_cuota,
    CAST(comentario AS STRING) AS comentario
FROM CTE



