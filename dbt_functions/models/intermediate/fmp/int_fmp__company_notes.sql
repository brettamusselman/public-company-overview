{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    Ticker,
    CIK,
    NoteTitle,
    NoteExchange,
    FileTimestamp,
    DBTLoadedAtStaging
FROM {{ ref('stg_fmp__company_notes') }}

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
