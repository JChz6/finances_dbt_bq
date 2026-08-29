{{
    config(
        materialized='view'
    )
}}

-- Expone la lista canónica del macro cuentas_alto_rendimiento() (ver macros/) como tabla real
-- en BigQuery, para que dashboard_api pueda hacer `cuenta IN (SELECT cuenta FROM
-- cuentas_alto_rendimiento)` en vez de hardcodear el literal en Python. El macro sigue siendo
-- la única fuente escrita a mano; este modelo solo la materializa para que algo fuera de dbt
-- también pueda leerla.
SELECT cuenta
FROM UNNEST({{ cuentas_alto_rendimiento() }}) AS cuenta
