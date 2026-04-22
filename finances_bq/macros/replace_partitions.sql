{% macro replace_partitions(source_ref, target_ref, date_src, date_tgt) %}
    {% if is_incremental() %}
        DELETE FROM {{ target_ref }}
        WHERE 
            DATE_TRUNC({{ date_tgt }}, MONTH)
        IN(
            SELECT DISTINCT
                DATE_TRUNC({{ date_src }}, MONTH)
            FROM {{ source_ref }}
            WHERE fecha_carga = (SELECT MAX(fecha_carga) FROM {{ source_ref }})
        )
    {% endif %}
{% endmacro %}