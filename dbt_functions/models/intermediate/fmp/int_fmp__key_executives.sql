{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    Ticker,
    ExecName,
    ExecTitle,
    ExecPay,
    PayCurrency,
    Gender,
    YearBorn,
    TitleSince,
    FileTimestamp,
    DBTLoadedAtStaging
FROM {{ ref('stg_fmp__key_executives') }}

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
