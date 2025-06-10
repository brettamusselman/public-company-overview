{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    Ticker,
    PeerTicker,
    PeerCompanyName,
    PeerPrice,
    PeerMarketCap,
    FileTimestamp,
    DBTLoadedAtStaging
FROM {{ ref('stg_fmp__stock_peers') }}

{% if is_incremental() %}
WHERE FileTimestamp >
      (
        SELECT COALESCE(
                 MAX(FileTimestamp),
                 PARSE_TIMESTAMP('%Y-%m-%d', '{{ var("past_proof_date") }}')
               )
        FROM {{ this }}
      )
{% endif %}
