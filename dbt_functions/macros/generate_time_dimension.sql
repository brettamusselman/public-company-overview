{% macro generate_static_time_dimension(pk_name="TimeDimKey", time_column_name="EventTime") %}
{#
  Generates a static time dimension table for every minute of a 24-hour day.

  Args:
    pk_name: The desired name for the primary key of this time dimension.
    time_column_name: The desired name for the main TIME type column in this dimension.
#}

{{
  config(
    materialized='table',
    unique_key=pk_name
  )
}}

WITH hours AS (
    SELECT hour FROM UNNEST(GENERATE_ARRAY(0, 23)) AS hour
),

minutes AS (
    SELECT minute FROM UNNEST(GENERATE_ARRAY(0, 59)) AS minute
),

seconds AS (
    SELECT second FROM UNNEST(GENERATE_ARRAY(0, 59)) AS second
),

time_spine AS (
    SELECT
        TIME(h.hour, m.minute, s.second) AS {{ time_column_name }} -- Creates TIME(hour, minute, seconds)
    FROM hours h
    CROSS JOIN minutes m
    CROSS JOIN seconds s 
)

SELECT
    CAST(REPLACE(FORMAT_TIME('%T', ts.{{ time_column_name }}), ':', '') AS INT64) AS {{ pk_name }},
    ts.{{ time_column_name }},
    EXTRACT(HOUR FROM ts.{{ time_column_name }}) AS HourOfDay,
    EXTRACT(MINUTE FROM ts.{{ time_column_name }}) AS MinuteOfHour,
    EXTRACT(SECOND FROM ts.{{ time_column_name }}) AS SecondOfMinute,
    CASE
        WHEN EXTRACT(HOUR FROM ts.{{ time_column_name }}) = 9 AND EXTRACT(MINUTE FROM ts.{{ time_column_name }}) >= 30 THEN TRUE -- Market open (9:30 AM)
        WHEN EXTRACT(HOUR FROM ts.{{ time_column_name }}) > 9 AND EXTRACT(HOUR FROM ts.{{ time_column_name }}) < 16 THEN TRUE    -- During market hours (until 4 PM)
        WHEN EXTRACT(HOUR FROM ts.{{ time_column_name }}) = 16 AND EXTRACT(MINUTE FROM ts.{{ time_column_name }}) = 0 THEN TRUE  -- Market close (4:00 PM)
        ELSE FALSE
    END AS IsMarketHoursNY,

    FORMAT_TIME('%I:%M %p', ts.{{ time_column_name }}) AS Time12HourFormat
FROM
    time_spine ts
ORDER BY
    ts.{{ time_column_name }}

{% endmacro %}