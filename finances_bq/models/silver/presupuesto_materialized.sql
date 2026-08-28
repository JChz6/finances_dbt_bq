WITH CTE AS (
    SELECT
        *
    FROM {{source('finances_raw', 'presupuesto')}}
)

SELECT
    CAST(fecha as DATE) AS fecha,
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    -- Normaliza nombres de categoría que llegan inconsistentes desde el Sheet de presupuesto:
    -- 'Seguro' (singular) vs 'Seguros' (resto del sistema, en fact_transactions) y
    -- 'Gastos variables' vs 'Gastos Variables' (mayúscula inconsistente). Sin esto, cualquier
    -- filtro exacto por nombre de categoría (p.ej. la lista canónica de esenciales) pierde
    -- silenciosamente estas filas.
    CASE
        WHEN UPPER(TRIM(categoria)) = 'SEGURO' THEN 'Seguros'
        WHEN UPPER(TRIM(categoria)) = 'GASTOS VARIABLES' THEN 'Gastos Variables'
        ELSE categoria
    END AS categoria,
    CAST(presupuesto AS NUMERIC) AS presupuesto
FROM CTE