{{
  config(
    materialized='table',
    unique_key='TimeDimKey'
  )
}}

WITH source_fmp AS (
    SELECT
        *
    FROM
        {{ ref('dim_fmp__time') }}
),

source_polygon AS (
    SELECT
        *
    FROM
        {{ ref('dim_polygon__time') }}
),

source_yfinance AS (
    SELECT
        *
    FROM
        {{ ref('dim_yfinance__time') }}
),

all_sources_combined AS (
    SELECT * FROM source_fmp
    UNION ALL
    SELECT * FROM source_polygon
    UNION ALL
    SELECT * FROM source_yfinance
)

SELECT DISTINCT
    *
FROM all_sources_combined