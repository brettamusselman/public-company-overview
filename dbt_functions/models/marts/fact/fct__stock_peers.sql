{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'PeerTickerDimKey', 'RecencyRank'],
    cluster_by   = ['TickerDimKey', 'PeerTickerDimKey']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__stock_peers') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

parent_join AS (
    SELECT
        dt.TickerDimKey    AS TickerDimKey,  
        s.*
    FROM src s
    LEFT JOIN dim_ticker dt
      ON s.Ticker = dt.Ticker
),

both_keys AS (
    SELECT
        p.*,
        dt2.TickerDimKey   AS PeerTickerDimKey  
    FROM parent_join p
    LEFT JOIN dim_ticker dt2
      ON p.PeerTicker = dt2.Ticker
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey, PeerTickerDimKey
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM both_keys
)

SELECT
    TickerDimKey,
    PeerTickerDimKey,
    PeerCompanyName,
    PeerPrice,
    PeerMarketCap,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
