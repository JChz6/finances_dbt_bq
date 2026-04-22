--TESTING: es imposible excederlo del 100%

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change = 'append_new_columns',
        unique_key = 'fecha_trunc',
        partition_by={
            "field": "fecha_trunc",
            "data_type": "datetime",
            "granularity": "month"
        },
        pre_hook = "{{ replace_partitions(ref('stg_transactions'), this, 'txn_time', 'txn_time') }}"
    )
}}


WITH FIRST_LAYER AS(
    SELECT
        DATE_TRUNC(txn_time, month) as fecha_trunc,
        EXTRACT(YEAR FROM txn_time) as anio,
        EXTRACT(MONTH FROM txn_time) as mes,
        ROUND((
            SUM(
            CASE
                WHEN categoria != "Reembolsos" AND ingreso_gasto = "Ingreso"
                THEN importe_moneda_principal
                ELSE 0
            END)), 2) as ingreso_total,
            --No es preciso y en negocios no calcula sobre el margen de beneficio real, solo da un aproximado aceptable del ingreso BRUTO del mes particionado
        ROUND(
            SUM(
            CASE
                WHEN ingreso_gasto = 'Gastos' AND categoria != 'Préstamos' AND subcategoria = 'Mujeres'
                THEN importe_moneda_principal
                ELSE 0
            END), 3) AS  gasto_mujeres,
        fecha_carga
    FROM {{ref('fact_transactions')}}importe_soles
    
    {% if is_incremental() %}
        WHERE fecha_carga > (SELECT MAX(fecha_carga) FROM {{ this }}) 
    {% endif %}
    
    GROUP BY ALL
)
SELECT
    fecha_trunc,
    anio,
    mes,
    ingreso_total,
    gasto_mujeres,
    CONCAT(
        ROUND((gasto_mujeres / ingreso_total)*100, 2)
        , '%') AS porc_mujeres,
    fecha_carga
FROM FIRST_LAYER