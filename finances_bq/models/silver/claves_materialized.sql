WITH CTE AS (
    SELECT
        *
    FROM {{source('finances_raw', 'claves')}}
)

SELECT 
    SAFE_CAST(numero AS INTEGER) AS numero,
    CAST(nombre AS STRING) AS nombre,
    CAST(clave AS STRING) AS clave,
    CAST(posicion AS STRING) AS posicion,
    CAST(descripcion AS STRING) AS descripcion,
    CAST(funcion_pandas AS STRING) AS funcion_pandas,
    SAFE_CAST(fecha_implementacion AS DATE) AS fecha_implementacion,
    CURRENT_TIMESTAMP AS fecha_carga
FROM CTE