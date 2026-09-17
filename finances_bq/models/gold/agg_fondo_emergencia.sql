{{
    config(
        materialized='view'
    )
}}

-- Tracking del fondo de emergencia (meta: macro fondo_emergencia_meta(), ver macros/fondo_emergencia.sql).
-- Desde 2026-09-09 el usuario etiqueta con 'Emerg/' (en cualquier posición del comentario, ver
-- fila 'Emergencia' en la Sheet de claves) las transacciones que retiran del fondo (imprevisto
-- real) o lo reponen (ingreso/reembolso). El match es por LIKE sobre el comentario crudo, no por
-- la columna `clave` de fact_transactions: esa columna solo captura la primera palabra del
-- comentario, y 'Emerg/' puede combinarse con otro tag (ej. "C/ Madre Emerg/"), así que un
-- comentario con 'Emerg/' en segunda posición quedaría invisible si filtráramos por `clave`.
WITH movimientos AS (
    SELECT
        txn_id,
        txn_time,
        DATE(txn_time) AS fecha,
        cuenta,
        categoria,
        subcategoria,
        concepto,
        comentario,
        ingreso_gasto,
        importe_moneda_principal,
        CASE
            WHEN ingreso_gasto LIKE '%ngres%' THEN importe_moneda_principal
            WHEN ingreso_gasto LIKE '%ast%' THEN -importe_moneda_principal
            ELSE 0
        END AS movimiento
    FROM {{ ref('fact_transactions') }}
    WHERE LOWER(comentario) LIKE '%emerg/%'
)

SELECT
    txn_id,
    txn_time,
    fecha,
    cuenta,
    categoria,
    subcategoria,
    concepto,
    comentario,
    ingreso_gasto,
    importe_moneda_principal,
    movimiento,
    {{ fondo_emergencia_meta() }} AS fondo_emergencia_meta,
    SUM(movimiento) OVER (
        ORDER BY txn_time, txn_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS movimiento_acumulado,
    {{ fondo_emergencia_meta() }} + SUM(movimiento) OVER (
        ORDER BY txn_time, txn_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS saldo_actual
FROM movimientos
ORDER BY txn_time, txn_id
