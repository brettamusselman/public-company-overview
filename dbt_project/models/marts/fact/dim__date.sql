{{
  config(
    materialized='table',
    unique_key='DateDimKey'
  )
}}

WITH source_fmp AS (
    SELECT
        *
    FROM
        {{ ref('dim_fmp__date') }}
),

source_polygon AS (
    SELECT
        *
    FROM
        {{ ref('dim_polygon__date') }}
),

source_yfinance AS (
    SELECT
        *
    FROM
        {{ ref('dim_yfinance__date') }}
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
