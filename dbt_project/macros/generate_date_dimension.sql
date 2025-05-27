{% macro generate_time_dimension(source_relation, datetime_column_name, pk_name="TimeDimKey", time_column_name="EventTime") %}

{{
  config(
    materialized='incremental',
    unique_key=pk_name
  )
}}

WITH source_events AS (
    SELECT
        {{ datetime_column_name }} AS _event_datetime_from_source
    FROM {{ source_relation }}
),

distinct_times_from_source AS (
    SELECT DISTINCT
        CAST(_event_datetime_from_source AS TIME) AS {{ time_column_name }}
    FROM source_events
),

times_to_build AS (
    SELECT
        src.{{ time_column_name }}
    FROM distinct_times_from_source src
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS dim_target
        ON src.{{ time_column_name }} = dim_target.{{ time_column_name }}
    WHERE dim_target.{{ time_column_name }} IS NULL 
    {% endif %}
)

SELECT
    CAST(REPLACE(FORMAT_TIME('%T', ttb.{{ time_column_name }}), ':', '') AS INT64) AS {{ pk_name }},
    ttb.{{ time_column_name }},                                                            
    EXTRACT(HOUR FROM ttb.{{ time_column_name }}) AS HourOfDay,        
    EXTRACT(MINUTE FROM ttb.{{ time_column_name }}) AS MinuteOfHour,   
    EXTRACT(SECOND FROM ttb.{{ time_column_name }}) AS SecondOfMinute,  
    CASE
        WHEN EXTRACT(HOUR FROM ttb.{{ time_column_name }}) = 9 AND EXTRACT(MINUTE FROM ttb.{{ time_column_name }}) >= 30 THEN TRUE
        WHEN EXTRACT(HOUR FROM ttb.{{ time_column_name }}) > 9 AND EXTRACT(HOUR FROM ttb.{{ time_column_name }}) < 16 THEN TRUE  
        WHEN EXTRACT(HOUR FROM ttb.{{ time_column_name }}) = 16 AND EXTRACT(MINUTE FROM ttb.{{ time_column_name }}) = 0 THEN TRUE 
        ELSE FALSE
    END AS IsMarketHoursNY,

    FORMAT_TIME('%I:%M %p', ttb.{{ time_column_name }}) AS Time12HourFormat
FROM
    times_to_build ttb

{% endmacro %}