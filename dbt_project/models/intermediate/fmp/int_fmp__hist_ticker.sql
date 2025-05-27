{{
  config(
    materialized='incremental',
    cluster_by=['Ticker', 'Inter', 'DateDimKey', 'TimeDimKey'],
  )
}}

WITH hist_ticker AS (
    SELECT *,
    FROM {{ ref('stg_fmp__hist_ticker') }}
),

dim_date_lookup AS (
    SELECT
        DateDimKey,
        EventDate         
    FROM
        {{ ref('dim_fmp__date') }}
),

dim_time_lookup AS (
    SELECT
        TimeDimKey,
        EventTime         
    FROM
        {{ ref('dim_fmp__time') }}
),

hist_ticker_with_dim_keys AS (
    SELECT
        bht.Ticker,
        bht.Close,
        bht.High,
        bht.Low,
        bht.Open,
        bht.Volume,
        bht.Vwap,
        bht.Inter,
        bht.Change,
        bht.ChangePercent,
        bht.FileTimestamp,
        bht.DBTLoadedAtStaging,
        ddl.DateDimKey,
        dtl.TimeDimKey
    FROM
        hist_ticker bht
    LEFT JOIN
        dim_date_lookup ddl
        ON CAST(bht.EventDateTime AS DATE) = ddl.EventDate
    LEFT JOIN
        dim_time_lookup dtl
        ON CAST(bht.EventDateTime AS TIME) = dtl.EventTime
)

SELECT * FROM hist_ticker_with_dim_keys
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}