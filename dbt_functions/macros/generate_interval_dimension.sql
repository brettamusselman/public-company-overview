{% macro generate_static_interval_dimension(
    pk_name="IntervalDimKey",
    interval_value_column_name="IntervalValue",
    base_unit_column_name="BaseUnit",
    multiplier_column_name="Multiplier",
    description_column_name="IntervalDescription",
    equivalent_in_minutes_col_name="EquivalentInMinutes"
) %}
{#
  Generates a static interval dimension table.

  Args:
    pk_name: The desired name for the primary key.
    interval_value_column_name: Name for the column holding the interval string (e.g., "15m", "4h").
    base_unit_column_name: Name for the column holding the base unit (e.g., "minute", "hour").
    multiplier_column_name: Name for the column holding the numeric multiplier.
    description_column_name: Name for the column holding a human-readable description.
    equivalent_in_minutes_col_name: Name for the column storing the intervals equivalent in total minutes.
#}

{{
  config(
    materialized='table',
    unique_key=pk_name
  )
}}

WITH interval_definitions AS (
    -- Values: IntervalValue, BaseUnit, Multiplier, Description, EquivalentInMinutes
    -- Minutes
    SELECT '1m' AS {{ interval_value_column_name }}, 'minute' AS {{ base_unit_column_name }}, 1 AS {{ multiplier_column_name }}, '1 Minute' AS {{ description_column_name }}, 1 AS {{ equivalent_in_minutes_col_name }} UNION ALL
    SELECT '2m', 'minute', 2, '2 Minutes', 2 UNION ALL
    SELECT '3m', 'minute', 3, '3 Minutes', 3 UNION ALL
    SELECT '4m', 'minute', 4, '4 Minutes', 4 UNION ALL
    SELECT '5m', 'minute', 5, '5 Minutes', 5 UNION ALL
    SELECT '10m', 'minute', 10, '10 Minutes', 10 UNION ALL
    SELECT '15m', 'minute', 15, '15 Minutes', 15 UNION ALL
    SELECT '30m', 'minute', 30, '30 Minutes', 30 UNION ALL
    SELECT '45m', 'minute', 45, '45 Minutes', 45 UNION ALL

    -- Hours (Multiplier * 60)
    SELECT '1h', 'hour', 1, '1 Hour', 1 * 60 UNION ALL
    SELECT '2h', 'hour', 2, '2 Hours', 2 * 60 UNION ALL
    SELECT '3h', 'hour', 3, '3 Hours', 3 * 60 UNION ALL
    SELECT '4h', 'hour', 4, '4 Hours', 4 * 60 UNION ALL
    SELECT '6h', 'hour', 6, '6 Hours', 6 * 60 UNION ALL
    SELECT '8h', 'hour', 8, '8 Hours', 8 * 60 UNION ALL
    SELECT '12h', 'hour', 12, '12 Hours', 12 * 60 UNION ALL

    -- Days (Multiplier * 24 * 60)
    SELECT '1d', 'day', 1, '1 Day', 1 * 24 * 60 UNION ALL
    SELECT '2d', 'day', 2, '2 Days', 2 * 24 * 60 UNION ALL
    SELECT '3d', 'day', 3, '3 Days', 3 * 24 * 60 UNION ALL
    SELECT '4d', 'day', 4, '4 Days', 4 * 24 * 60 UNION ALL
    SELECT '5d', 'day', 5, '5 Days', 5 * 24 * 60 UNION ALL
    SELECT '6d', 'day', 6, '6 Days', 6 * 24 * 60 UNION ALL

    -- Weeks (Multiplier * 7 * 24 * 60)
    SELECT '1wk', 'week', 1, '1 Week', 1 * 7 * 24 * 60 UNION ALL
    SELECT '2wk', 'week', 2, '2 Weeks', 2 * 7 * 24 * 60 UNION ALL
    SELECT '3wk', 'week', 3, '3 Weeks', 3 * 7 * 24 * 60 UNION ALL
    SELECT '4wk', 'week', 4, '4 Weeks', 4 * 7 * 24 * 60 UNION ALL
    SELECT '13wk', 'week', 13, '1 Quarter (13 Weeks)', 13 * 7 * 24 * 60 UNION ALL
    SELECT '26wk', 'week', 26, 'Half Year (26 Weeks)', 26 * 7 * 24 * 60 UNION ALL
    SELECT '52wk', 'week', 52, '1 Year (52 Weeks)', 52 * 7 * 24 * 60 UNION ALL

    -- Months (Multiplier * 30 days * 24 hours * 60 minutes - Approximation)
    SELECT '1mo', 'month', 1, '1 Month', 1 * 30 * 24 * 60 UNION ALL
    SELECT '2mo', 'month', 2, '2 Months', 2 * 30 * 24 * 60 UNION ALL
    SELECT '3mo', 'month', 3, '1 Quarter (3 Months)', 3 * 30 * 24 * 60 UNION ALL
    SELECT '4mo', 'month', 4, '4 Months', 4 * 30 * 24 * 60 UNION ALL
    SELECT '6mo', 'month', 6, 'Half Year (6 Months)', 6 * 30 * 24 * 60 UNION ALL
    SELECT '9mo', 'month', 9, '9 Months', 9 * 30 * 24 * 60 UNION ALL

    -- Quarters (Multiplier * 3 months * 30 days * 24 hours * 60 minutes - Approximation)
    SELECT '1qtr', 'quarter', 1, '1 Quarter', 1 * 3 * 30 * 24 * 60 UNION ALL
    SELECT '2qtr', 'quarter', 2, '2 Quarters (Half Year)', 2 * 3 * 30 * 24 * 60 UNION ALL
    SELECT '3qtr', 'quarter', 3, '3 Quarters', 3 * 3 * 30 * 24 * 60 UNION ALL
    SELECT '4qtr', 'quarter', 4, '1 Year (4 Quarters)', 4 * 3 * 30 * 24 * 60 UNION ALL

    -- Years (Multiplier * 365 days * 24 hours * 60 minutes - Approximation, ignores leap years)
    SELECT '1yr', 'year', 1, '1 Year', 1 * 365 * 24 * 60 UNION ALL
    SELECT '2yr', 'year', 2, '2 Years', 2 * 365 * 24 * 60 UNION ALL
    SELECT '3yr', 'year', 3, '3 Years', 3 * 365 * 24 * 60 UNION ALL
    SELECT '4yr', 'year', 4, '4 Years', 4 * 365 * 24 * 60 UNION ALL
    SELECT '5yr', 'year', 5, '5 Years', 5 * 365 * 24 * 60 UNION ALL
    SELECT '10yr', 'year', 10, '10 Years', 10 * 365 * 24 * 60
)

SELECT
    {{ dbt_utils.generate_surrogate_key([interval_value_column_name]) }} AS {{ pk_name }},
    {{ interval_value_column_name }},
    {{ base_unit_column_name }},
    {{ multiplier_column_name }},
    {{ description_column_name }},
    CAST({{ equivalent_in_minutes_col_name }} AS INT64) AS {{ equivalent_in_minutes_col_name }} 
FROM
    interval_definitions
ORDER BY
    {{ equivalent_in_minutes_col_name }}, {{ multiplier_column_name }}

{% endmacro %}