{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'ExecNameAndPosition', 'CompYear', 'RecencyRank'],
    cluster_by   = ['TickerDimKey', 'CompYear']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__exec_comp') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        d.TickerDimKey,
        s.ExecNameAndPosition,
        s.CompYear,
        s.Salary,
        s.Bonus,
        s.StockAward,
        s.OptionAward,
        s.IncentivePlanComp,
        s.AllOtherComp,
        s.TotalComp,
        s.SourceURL,
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
            PARTITION BY TickerDimKey, ExecNameAndPosition, CompYear
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    TickerDimKey,
    ExecNameAndPosition,
    CompYear,
    Salary,
    Bonus,
    StockAward,
    OptionAward,
    IncentivePlanComp,
    AllOtherComp,
    TotalComp,
    SourceURL,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
