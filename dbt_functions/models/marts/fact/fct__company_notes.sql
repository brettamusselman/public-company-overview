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

dim_exchange AS (
    SELECT 
        ExchangeDimKey, 
        ExchangeAcronym,
        SourceSystem
    FROM {{ ref('dim__exchanges') }}
),

joined AS (
    SELECT
        t.TickerDimKey,
        e.ExchangeDimKey,           
        s.NoteTitle,
        s.NoteExchange,
        s.CIK,
        s.FileTimestamp,
        s.DBTLoadedAtStaging,
        s.DataSource
    FROM src s
    LEFT JOIN dim_ticker   t ON s.Ticker       = t.Ticker
    LEFT JOIN dim_exchange e 
        ON s.NoteExchange = e.ExchangeAcronym
        AND e.SourceSystem = s.DataSource 
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
    ExchangeDimKey,
    NoteTitle,
    CIK,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
