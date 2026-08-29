{{
    config(
        materialized='incremental',
        on_schema_change = 'append_new_columns',
        incremental_strategy='merge',
        unique_key = 'mes_cat_id',
        partition_by={
            "field": "mes",
            "data_type": "datetime",
            "granularity": "month"
        },
        pre_hook = "{{ replace_partitions(ref('fact_transactions'), this, 'txn_time', 'mes') }}"
    )
}}

-- Grano mes x categoria de gasto real, para comparativas year-over-year (crecimiento de
-- ingreso neto vs. agg_ingresos, "inflación de estilo de vida" por categoría). Sin whitelist:
-- cubre todas las categorías tal cual vienen en fact_transactions (sin normalizar mayúsculas/
-- espacios — ese problema solo existe en presupuesto_materialized, no acá) y sin el neteo de
-- Anuncios/Préstamos contra Reembolsos (ver agg_netos_prestamos_anuncios) — ambos se aplican
-- en dashboard_api, igual que hoy en /gastos-categoria, para no duplicar esa lógica en dos capas.
--
-- Incremental (no table) porque no hay join: a diferencia de agg_cumplimiento_presupuesto
-- (FULL OUTER JOIN contra presupuesto_materialized), acá mes nunca puede salir NULL desde un
-- solo lado, así que no aplica el riesgo de unique_key NULL de ese patrón. replace_partitions
-- solo re-escribe los meses del batch más reciente de fact_transactions en vez de recomputar
-- toda la tabla en cada build, lo que importa cada vez más a medida que crece el historial.

SELECT
    TO_HEX(MD5(CONCAT(
        CAST(DATE_TRUNC(txn_time, MONTH) AS STRING),
        categoria
    ))) AS mes_cat_id,
    DATE_TRUNC(txn_time, MONTH) AS mes,
    categoria,
    SUM(importe_moneda_principal) AS monto_real,
    MAX(fecha_carga) AS fecha_carga
FROM {{ ref('fact_transactions') }}
WHERE ingreso_gasto = 'Gastos'
GROUP BY 1, 2, 3
