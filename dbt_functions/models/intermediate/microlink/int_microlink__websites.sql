{{
  config(
    materialized = 'incremental',
  )
}}

WITH meta AS (
    SELECT *
    FROM {{ ref('stg_microlink__text') }}
),

combined AS (
    SELECT
        meta.*,
    FROM meta
)

SELECT
    *                                     
FROM combined

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
