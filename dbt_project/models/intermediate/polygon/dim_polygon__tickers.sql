{{
  config(
    materialized='incremental',
    unique_key='TickerDimKey'
  )
}}

WITH tickers AS (
    SELECT
        *
    FROM {{ ref('stg_polygon__tickers') }}
    WHERE Market='stocks' OR Market='otc'
),

dim_exchanges_lookup AS (
    SELECT
        ExchangeDimKey, 
        MIC  
    FROM {{ ref('dim_polygon__exchanges') }} 
),

tickers_joined_with_exchange_dim AS (
    SELECT
        stg_tickers.Ticker,
        stg_tickers.CompanyName,
        stg_tickers.FileTimestamp,
        stg_tickers.DBTLoadedAtStaging,
        exchanges.ExchangeDimKey
    FROM tickers stg_tickers
    LEFT JOIN dim_exchanges_lookup exchanges
        ON stg_tickers.PrimaryExchange = exchanges.MIC
)

SELECT
    Ticker AS TickerDimKey,
    Ticker,
    CompanyName,
    FileTimestamp,
    DBTLoadedAtStaging,
    ExchangeDimKey
FROM tickers_joined_with_exchange_dim
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}