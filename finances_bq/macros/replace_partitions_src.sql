{% macro replace_partitions_src(source_ref, target_ref, file_name, date_tgt) %}
    {% if is_incremental() %}
        DELETE FROM {{ target_ref }}
        WHERE 
            DATE_TRUNC({{date_tgt}}, MONTH)
        IN(
            SELECT DISTINCT
                DATETIME_TRUNC(PARSE_DATETIME('%y%m%d', CONCAT(REPLACE(RIGHT({{file_name}}, 8), '.csv', ''), '01')), MONTH),
            FROM {{ source_ref }}
            WHERE updated = (SELECT MAX(updated) FROM {{ source_ref }})
        )
    {% endif %}
{% endmacro %}