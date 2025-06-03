{{
  config(
    materialized='incremental',
    cluster_by=['Ticker', 'Inter']
  )
}}

WITH hist_ticker AS (
    SELECT
        bht.Ticker,
        bht.EventDateTime,
        bht.Inter,
        bht.Open,
        bht.High,
        bht.Low,
        bht.Close,
        bht.Volume,
        bht.Dividends,
        bht.StockSplits,
        bht.FileTimestamp,
        bht.DBTLoadedAtStaging
    FROM {{ ref('stg_yfinance__hist_ticker') }} bht
)

SELECT * FROM hist_ticker
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}