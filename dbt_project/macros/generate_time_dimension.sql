{% macro generate_date_dimension(source_relation, datetime_column_name, pk_name="DateDimKey", date_column_name="EventDate") %}

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

distinct_dates_from_source AS (
    SELECT DISTINCT
        CAST(_event_datetime_from_source AS DATE) AS {{ date_column_name }}
    FROM source_events
),

dates_to_build AS (
    SELECT
        src.{{ date_column_name }}
    FROM distinct_dates_from_source src
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS dim_target
        ON src.{{ date_column_name }} = dim_target.{{ date_column_name }}
    WHERE dim_target.{{ date_column_name }} IS NULL 
    {% endif %}
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', dtb.{{ date_column_name }}) AS INT64) AS {{ pk_name }},
    dtb.{{ date_column_name }},
    EXTRACT(YEAR FROM dtb.{{ date_column_name }}) AS YearNumber,
    EXTRACT(QUARTER FROM dtb.{{ date_column_name }}) AS QuarterOfYear,
    EXTRACT(MONTH FROM dtb.{{ date_column_name }}) AS MonthOfYear,
    EXTRACT(DAY FROM dtb.{{ date_column_name }}) AS DayOfMonth,
    EXTRACT(DAYOFWEEK FROM dtb.{{ date_column_name }}) AS DayOfWeekNumber, -- 1 (Sunday) to 7 (Saturday)
    FORMAT_DATE('%A', dtb.{{ date_column_name }}) AS DayName,
    FORMAT_DATE('%B', dtb.{{ date_column_name }}) AS MonthName,
    EXTRACT(WEEK FROM dtb.{{ date_column_name }}) AS WeekOfYearISO,
    EXTRACT(DAYOFYEAR FROM dtb.{{ date_column_name }}) AS DayOfYear,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM dtb.{{ date_column_name }}) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS IsWeekend
FROM
    dates_to_build dtb

{% endmacro %}