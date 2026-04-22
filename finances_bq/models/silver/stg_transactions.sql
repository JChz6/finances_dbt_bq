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
        pre_hook = "{{
            replace_partitions_src(
                source('finances_raw',
                'raw_transactions_metadata'),
                this,
                'uri',
                'txn_time')
            }}"
    )
}}


WITH CTE AS (
    SELECT 
        txn.*,
        mdt.uri AS src_file,
        TIMESTAMP_MICROS(mdt.generation) AS file_creation_ts,
        mdt.size AS file_size,
        mdt.updated AS fecha_carga
    FROM
    {{source('finances_raw', 'raw_transactions')}} txn
    LEFT JOIN
    {{source('finances_raw', 'raw_transactions_metadata')}} mdt
    ON txn._FILE_NAME = mdt.uri

    {% if is_incremental() %}
        WHERE mdt.updated > (SELECT MAX(fecha_carga) FROM {{ this }}) 
    {% endif %}
)

SELECT 
    TO_HEX(MD5(
        CONCAT(
        CAST(segun_un_periodo AS STRING),
        CAST(cuentas AS STRING),
        CAST(pen AS STRING),
        COALESCE(nota, 'transfer'),
        COALESCE(descripcion, 'null'),
        categoria,
        COALESCE(subcategorias, 'null')
        )
    )) AS txn_id,
    SAFE_CAST(FORMAT_DATE('%Y%m', DATE(segun_un_periodo)) AS INT64) AS month_id,
    DATETIME(segun_un_periodo) AS txn_time,
    FORMAT_DATE('%A', DATETIME(segun_un_periodo)) AS day_of_week,
    SAFE_CAST(cuentas AS STRING) AS cuenta,
    SAFE_CAST(categoria AS STRING) AS categoria,
    SAFE_CAST(subcategorias AS STRING) AS subcategoria,
    SAFE_CAST(nota AS STRING) AS concepto,
    SAFE_CAST(pen AS NUMERIC) AS importe_moneda_principal,
    SAFE_CAST(ingreso_gasto AS STRING) AS ingreso_gasto,
    SAFE_CAST(descripcion AS STRING) AS comentario,
    SAFE_CAST(importe AS NUMERIC) AS importe_nativo,
    SAFE_CAST(moneda AS STRING) AS moneda,
    SAFE_CAST(cuentas_1 AS NUMERIC) AS importe_txn,
    src_file,
    file_size,
    file_creation_ts,
    fecha_carga 
FROM CTE