{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    *
FROM {{ ref('stg_fmp__company_profile') }}

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
