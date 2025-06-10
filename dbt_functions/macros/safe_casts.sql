{% macro to_int(col) %}
SAFE_CAST(CAST(SAFE_CAST({{ col }} AS FLOAT64) AS INT64) AS INT64)
{% endmacro %}

{% macro to_float(col) %}
SAFE_CAST({{ col }} AS FLOAT64)
{% endmacro %}

{% macro to_bool(col) %}
CAST(LOWER({{ col }}) = 'true' AS BOOL)
{% endmacro %}