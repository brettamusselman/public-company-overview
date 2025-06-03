{% macro to_int(col) %}
SAFE_CAST(CAST(SAFE_CAST({{ col }} AS FLOAT64) AS INT64) AS INT64)
{% endmacro %}