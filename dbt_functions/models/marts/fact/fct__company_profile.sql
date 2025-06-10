{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'RecencyRank'],
    cluster_by   = ['TickerDimKey']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__company_profile') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        d.TickerDimKey,
        s.* EXCEPT(Ticker)
    FROM src s
    LEFT JOIN dim_ticker d
      ON s.Ticker = d.Ticker
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    *
FROM ranked
WHERE RecencyRank = 1
