{% macro generate_static_date_dimension(start_year=1900, end_year=2100, pk_name="DateDimKey", date_column_name="EventDate") %}
{#
  Generates a static date dimension table for a given range of years.

  Args:
    start_year: The first year to include in the dimension (inclusive).
    end_year: The last year to include in the dimension (inclusive).
    pk_name: The desired name for the primary key of this date dimension.
    date_column_name: The desired name for the main DATE type column in this dimension.
#}

{{
  config(
    materialized='table',  
    unique_key=pk_name
  )
}}

WITH date_spine AS (
  {{ dbt_utils.date_spine(
      datepart="day",
      start_date="cast('" ~ start_year ~ "-01-01' as date)",
      end_date="cast('" ~ end_year ~ "-12-31' as date)"
     )
  }}
)

SELECT
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS {{ pk_name }},
    DATE(date_day) AS {{ date_column_name }},
    EXTRACT(YEAR FROM date_day) AS YearNumber,
    EXTRACT(QUARTER FROM date_day) AS QuarterOfYear,
    EXTRACT(MONTH FROM date_day) AS MonthOfYear,
    EXTRACT(DAY FROM date_day) AS DayOfMonth,
    EXTRACT(DAYOFWEEK FROM date_day) AS DayOfWeekNumber, -- 1 (Sunday) to 7 (Saturday)
    FORMAT_DATE('%A', date_day) AS DayName,
    FORMAT_DATE('%B', date_day) AS MonthName,
    EXTRACT(WEEK FROM date_day) AS WeekOfYearISO,
    EXTRACT(DAYOFYEAR FROM date_day) AS DayOfYear,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE
        ELSE FALSE
    END AS IsWeekend
FROM
    date_spine
ORDER BY
    date_day 

{% endmacro %}