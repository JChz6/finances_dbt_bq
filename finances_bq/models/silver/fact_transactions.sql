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
        pre_hook = "{{ replace_partitions(ref('stg_transactions'), this, 'txn_time', 'txn_time') }}"
    )
}}


WITH CTE AS(
    SELECT
        *,
        SPLIT(comentario, ' ')[SAFE_OFFSET(0)] AS part_1,
        SPLIT(comentario, ' ')[SAFE_OFFSET(1)] AS part_2
    FROM {{ ref('stg_transactions') }}
    WHERE cuenta NOT IN ('Kilometraje', 'Personal')

    {% if is_incremental() %}
        AND fecha_carga > (SELECT MAX(fecha_carga) FROM {{ this }}) 
    {% endif %}

),

FIRST_LAYER AS(

    SELECT
        txn_id,
        month_id,
        txn_time,
        day_of_week,
        cuenta,
        categoria,
        subcategoria,
        concepto,
        importe_moneda_principal,
        ingreso_gasto,
        comentario,
        importe_nativo,
        moneda,
        importe_txn,
        SAFE_CAST(ROUND(SAFE_DIVIDE(importe_moneda_principal, importe_txn), 3) AS NUMERIC) AS tipo_cambio,
        CASE
            WHEN TRIM(REPLACE(LOWER(comentario), 'í', 'i')) LIKE '%dias trabajados'
                THEN CAST(part_1 AS NUMERIC)
            WHEN TRIM(REPLACE(LOWER(comentario), 'í', 'i')) LIKE '%dia trabajado'
                THEN CAST(part_1 AS NUMERIC)
            ELSE NULL
        END AS dias_trabajados,
    
        CASE
            WHEN part_1 IN (SELECT DISTINCT clave FROM {{ref('claves')}})
                THEN part_1
            ELSE NULL
        END AS clave,
    
        fecha_carga,
        src_file,
        file_creation_ts,
        part_1,
        part_2
    FROM CTE
)
SELECT
    txn_id,
    month_id,
    txn_time,
    day_of_week,
    cuenta,
    categoria,
    subcategoria,
    concepto,
    importe_moneda_principal,
    ingreso_gasto,
    comentario,
    importe_nativo,
    moneda,
    importe_txn,
    tipo_cambio,
    dias_trabajados,
    clave,
    CASE
        WHEN clave = 'C/' THEN part_2
        WHEN clave IS NOT NULL AND clave != 'C/' THEN TRIM(SPLIT(comentario, '/')[SAFE_OFFSET(1)])
        ELSE NULL
    END AS valor,
    src_file,
    file_creation_ts,
    fecha_carga
FROM FIRST_LAYER