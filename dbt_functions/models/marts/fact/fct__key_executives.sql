{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'ExecName', 'RecencyRank'],
    cluster_by   = ['TickerDimKey']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__key_executives') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        d.TickerDimKey,
        s.ExecName,
        s.ExecTitle,
        s.ExecPay,
        s.PayCurrency,
        s.Gender,
        s.YearBorn,
        s.TitleSince,
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
            PARTITION BY TickerDimKey, ExecName
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    TickerDimKey,
    ExecName,
    ExecTitle,
    ExecPay,
    PayCurrency,
    Gender,
    YearBorn,
    TitleSince,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
