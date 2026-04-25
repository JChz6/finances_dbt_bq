SELECT
    *
FROM {{ ref('fact_transactions') }}
WHERE clave IS NOT NULL AND valor IS NULL