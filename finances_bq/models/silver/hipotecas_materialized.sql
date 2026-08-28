WITH CTE AS (
    SELECT
        *
    FROM {{source('finances_raw', 'hip_depa_alameda_dolores')}}
),

base AS (
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
),

-- Propósito vigente del inmueble en la fecha de vencimiento de cada cuota (Sheet
-- tracking_inmuebles, un rango [fecha_inicio, fecha_fin] por propósito; fecha_fin NULL =
-- vigente). Si una cuota no cae dentro de ningún rango (p.ej. cuotas anteriores al primer
-- registro de tracking_inmuebles), queda sin propósito y sin clasificar en ninguno de los
-- dos buckets de abajo — decisión explícita del usuario, no se asume vivienda propia por
-- defecto para no inventar dato histórico que no existe en el tracker.
-- El Sheet trae filas en blanco (arrastre de sheet_range) y espacios sobrantes en texto
-- (confirmado: 'Vivienda propia ' con espacio final) — TRIM en ambos lados o el match
-- exacto contra 'Vivienda propia' falla en silencio y todo queda sin clasificar.
tracking AS (
    SELECT
        TRIM(inmueble) AS inmueble,
        fecha_inicio,
        fecha_fin,
        TRIM(proposito) AS proposito
    FROM {{ source('finances_raw', 'tracking_inmuebles') }}
    WHERE inmueble IS NOT NULL
)

SELECT
    b.*,
    t.proposito AS proposito_vigente,
    -- capital_cuota NO se toca acá: es patrimonio siempre, sin importar el propósito.
    -- interés + seguros sí se separa: consumo real (costo de vida) si el inmueble es
    -- vivienda propia en la fecha de la cuota, costo operativo de una inversión de renta si
    -- no. Ver macros/categorias_esenciales.sql para dónde entra costo_vida_interes_seguros.
    IF(
        t.proposito = 'Vivienda propia',
        b.interes_cuota + b.seguro_propiedad + b.seguro_desgravamen,
        0
    ) AS costo_vida_interes_seguros,
    IF(
        t.proposito IS NOT NULL AND t.proposito != 'Vivienda propia',
        b.interes_cuota + b.seguro_propiedad + b.seguro_desgravamen,
        0
    ) AS costo_operativo_inversion_interes_seguros
FROM base b
LEFT JOIN tracking t
    ON b.inmueble_id = t.inmueble
    AND b.fecha_vencimiento BETWEEN t.fecha_inicio AND COALESCE(t.fecha_fin, DATE '9999-12-31')
