{{
    config(
        materialized='incremental',
        on_schema_change = 'append_new_columns',
        incremental_strategy='insert_overwrite',
        partition_by={
            "field": "txn_time",
            "data_type": "datetime",
            "granularity": "month"
        },
        pre_hook = "{{ replace_partitions(ref('fact_transactions'), this, 'txn_time', 'txn_time') }}"
    )
}}

WITH CTE AS(
    SELECT
        *
    FROM {{ref('fact_transactions')}}
    WHERE clave = 'Inm/'
    AND categoria NOT IN ('Comida', 'Regalos', 'Transporte')
    
    {% if is_incremental() %}
        AND fecha_carga > (SELECT MAX(fecha_carga) FROM {{ this }}) 
    {% endif %}
)

SELECT
    txn_id,
    month_id,
    txn_time,
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
    clave,
    valor,
    fecha_carga
FROM CTE