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
--
-- ingreso_gasto distingue gasto/ingreso real ('Gastos'/'Ingreso') de una pata de transferencia
-- entre cuentas propias ('Dinero gastado'/'Dinero ingresado'). Una transferencia se registra como
-- dos filas (una por cuenta); si ambas patas se etiquetan 'Emerg/' y se contaran las dos, se
-- cancelan a 0 aunque el fondo sí se movió (bug real, caso txn c749200482e8/9a6d2bfedb1).
-- El fondo vive físicamente dentro de cuentas_alto_rendimiento (ver macro), así que solo la pata
-- de transferencia que toca una de esas cuentas representa un movimiento real del fondo; la otra
-- pata (dónde terminó/de dónde salió el efectivo) no es parte del fondo y se ignora. Si la
-- transferencia es entre dos cuentas_alto_rendimiento, ambas patas cuentan y se cancelan
-- correctamente (el fondo no cambió de tamaño, solo de cuenta).
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
            WHEN ingreso_gasto = 'Ingreso' THEN importe_moneda_principal
            WHEN ingreso_gasto = 'Gastos' THEN -importe_moneda_principal
            WHEN ingreso_gasto = 'Dinero ingresado' AND cuenta IN UNNEST({{ cuentas_alto_rendimiento() }}) THEN importe_moneda_principal
            WHEN ingreso_gasto = 'Dinero gastado' AND cuenta IN UNNEST({{ cuentas_alto_rendimiento() }}) THEN -importe_moneda_principal
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
