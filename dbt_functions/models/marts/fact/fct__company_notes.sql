{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'NoteTitle', 'RecencyRank'],
    cluster_by   = ['TickerDimKey']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__company_notes') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        d.TickerDimKey,
        s.NoteTitle,
        s.NoteExchange,
        s.CIK,
        s.FileTimestamp,
        s.DBTLoadedAtStaging,
        s.DataSource
    FROM src s
    LEFT JOIN dim_ticker d
      ON s.Ticker = d.Ticker
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey, NoteTitle
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    TickerDimKey,
    NoteTitle,
    NoteExchange,
    CIK,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
