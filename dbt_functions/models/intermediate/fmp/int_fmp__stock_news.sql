{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    Ticker,
    PublishedDateTime,
    Publisher,
    Title,
    ImageURL,
    Site,
    ArticleText,
    ArticleURL,
    FileTimestamp,
    DBTLoadedAtStaging
FROM {{ ref('stg_fmp__stock_news') }}

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
