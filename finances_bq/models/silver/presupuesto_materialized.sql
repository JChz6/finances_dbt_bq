WITH CTE AS (
    SELECT
        *
    FROM {{source('finances_raw', 'presupuesto')}}
)

SELECT 
    CAST(fecha as DATE) AS fecha,
    CAST(year AS INT64) AS year,
    CAST(month AS INT64) AS month,
    categoria,
    CAST(presupuesto AS NUMERIC) AS presupuesto
FROM CTE