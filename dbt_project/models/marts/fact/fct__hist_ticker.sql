{{
  config(
    materialized='table',
    cluster_by=['Ticker', 'Inter'],
    unique_key=['Ticker', 'Datetime', 'Inter', 'Recency'],
    partition_by={
      "field": "Datetime",
      "data_type": "datetime",
      "granularity": "day"
    }
  )
}}

WITH source_fmp AS (
    SELECT
        `Close`,
        High,
        Low,
        `Open`,
        Volume,
        Ticker,        
        Vwap,
        NULL AS Dividends,
        NULL AS StockSplits,
        NULL AS NumTransactions,
        Change,
        ChangePercent,
        '1d' AS Inter, 
        `Datetime`, 
        FileTimestamp,
        DBTLoadedAtStaging,
        'FMP' AS DataSource
    FROM
        {{ ref('int_fmp__hist_ticker') }}
),

source_polygon AS (
    SELECT
        `Close`,
        High,
        Low,
        `Open`,
        Volume,
        Ticker,     
        Vwap,
        NULL AS Dividends,
        NULL AS StockSplits,
        NumTransactions,
        NULL AS Change,
        NULL AS ChangePercent,
        Inter,           
        `Datetime`,     
        FileTimestamp,
        DBTLoadedAtStaging,
        'Polygon' AS DataSource
    FROM
        {{ ref('int_polygon__hist_ticker') }}
),

source_yfinance AS (
    SELECT
        `Close`,
        High,
        Low,
        `Open`,
        Volume,
        Ticker,
        NULL AS Vwap,        
        Dividends,
        StockSplits,
        NULL AS NumTransactions,
        NULL AS Change,
        NULL AS ChangePercent,
        Inter,          
        `Datetime`,  
        FileTimestamp,
        DBTLoadedAtStaging,
        'YFinance' AS DataSource
    FROM
        {{ ref('int_yfinance__hist_ticker') }}
),

all_sources_combined AS (
    SELECT * FROM source_fmp
    UNION ALL
    SELECT * FROM source_polygon
    UNION ALL
    SELECT * FROM source_yfinance
),

ranked_combined_data AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY Ticker, `Datetime`, Inter
            ORDER BY FileTimestamp DESC
        ) as RecencyRank
    FROM
        all_sources_combined
)

SELECT
    *
FROM
    ranked_combined_data
WHERE
    RecencyRank = 1
