{{
  config(
    materialized='table',
    cluster_by=["Ticker"]
  )
}}

WITH yf_ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY Ticker
            ORDER BY FileTimestamp DESC
        ) as Recency
    FROM
        {{ ref('int_yfinance__hist_ticker') }}
)

SELECT
    *
FROM
    yf_ranked
WHERE
    Recency = 1
