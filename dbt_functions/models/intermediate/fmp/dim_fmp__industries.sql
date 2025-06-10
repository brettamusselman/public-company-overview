{{
  config(
    materialized = 'incremental',
    unique_key   = 'IndustryDimKey'
  )
}}

WITH industries AS (
    SELECT
        IndustryName,
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
            'America/New_York'
        )                 AS FileTimestamp,
        CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
    FROM {{ ref('stg_fmp__industries') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['IndustryName']) }} AS IndustryDimKey,
    IndustryName,
    FileTimestamp,
    DBTLoadedAtStaging
FROM industries

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
