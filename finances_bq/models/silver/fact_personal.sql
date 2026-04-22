{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change = 'append_new_columns',
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
        txn_id,
        month_id,
        txn_time,
        day_of_week,
        cuenta,
        categoria,
        subcategoria,
        concepto,
        CASE 
            WHEN LOWER(ingreso_gasto) LIKE '%ingr%' THEN 'Positivo'
            WHEN LOWER(ingreso_gasto) LIKE '%gast%' THEN 'Negativo'
            ELSE NULL
        END AS sentimiento,
        comentario,
        importe_nativo,
        moneda,
        importe_txn,
        SPLIT(comentario, ' ')[SAFE_OFFSET(0)] AS part_1,
        SPLIT(comentario, ' ')[SAFE_OFFSET(1)] AS part_2,
        fecha_carga
    FROM {{ref('stg_transactions')}}
    WHERE cuenta IN ('Personal')

    {% if is_incremental() %}
        AND fecha_carga > (SELECT MAX(fecha_carga) FROM {{ this }}) 
    {% endif %}

), FIRST_LAYER AS(
    SELECT
        txn_id,
        month_id,
        txn_time,
        day_of_week,
        cuenta,
        categoria,
        subcategoria,
        concepto,
        sentimiento,
        comentario,
        importe_nativo,
        moneda,
        importe_txn,
        CASE
            WHEN part_1 IN (SELECT DISTINCT clave FROM {{ref('claves')}})
                THEN part_1
            ELSE NULL
        END AS clave,
        fecha_carga,
        part_1,
        part_2
    FROM CTE
)
SELECT
    txn_id,
    month_id,
    txn_time,
    cuenta,
    categoria,
    subcategoria,
    concepto,
    sentimiento,
    comentario,
    importe_nativo,
    moneda,
    importe_txn,
    clave,
    CASE
        WHEN clave = 'C/' THEN part_2
        WHEN clave IS NOT NULL AND clave != 'C/' THEN TRIM(SPLIT(comentario, '/')[SAFE_OFFSET(1)])
        ELSE NULL
    END AS valor,
    fecha_carga
FROM FIRST_LAYER

