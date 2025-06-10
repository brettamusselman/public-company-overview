{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'DateDimKey', 'RecencyRank'],
    cluster_by   = ['TickerDimKey', 'DateDimKey']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__employee_count') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

dim_date AS (
    SELECT DateDimKey, EventDate
    FROM {{ ref('dim__date') }}
),

joined AS (
    SELECT
        t.TickerDimKey,
        d.DateDimKey,
        s.EmployeeCount,
        s.FormType,
        s.SourceURL,
        s.FileTimestamp,
        s.DBTLoadedAtStaging,
        s.DataSource
    FROM src s
    LEFT JOIN dim_ticker t ON s.Ticker         = t.Ticker
    LEFT JOIN dim_date   d ON s.PeriodOfReport = d.EventDate
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey, DateDimKey
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    TickerDimKey,
    DateDimKey,
    EmployeeCount,
    FormType,
    SourceURL,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
