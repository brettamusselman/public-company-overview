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
        bht.Close,
        bht.High,
        bht.Low,
        bht.Open,
        bht.Volume,
        bht.Vwap,
        bht.NumTransactions,
        bht.Inter,
        bht.FileTimestamp,
        bht.DBTLoadedAtStaging
    FROM {{ ref('stg_polygon__hist_ticker') }} bht
)

SELECT * FROM hist_ticker
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}