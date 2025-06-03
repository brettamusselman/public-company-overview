{{
  config(
    materialized='table',
    unique_key='TickersDimKey'
  )
}}

WITH fmp_derived_tickers AS (
    SELECT
        *,
        'FMP' AS SourceSystem,
        1 AS source_priority
    FROM {{ ref('dim_fmp__tickers') }}
),

polygon_derived_tickers AS (
    SELECT
        *,
        'Polygon' AS SourceSystem,
        2 AS source_priority
    FROM {{ ref('dim_polygon__tickers') }} 
),

all_derived_tickers_unioned AS (
    SELECT * FROM fmp_derived_tickers
    UNION ALL
    SELECT * FROM polygon_derived_tickers
),

final_ranked_tickers AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey
            ORDER BY source_priority ASC, FileTimestamp DESC
        ) as rn
    FROM all_derived_tickers_unioned
)

SELECT
    TickerDimKey,
    Ticker,
    CompanyName,
    SourceSystem,
    FileTimestamp,
    DBTLoadedAtStaging,
    ExchangeDimKey,
    rn AS Rank       
FROM final_ranked_tickers
WHERE rn=1