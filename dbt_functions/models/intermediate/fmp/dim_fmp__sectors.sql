{{
  config(
    materialized = 'incremental',
    unique_key   = 'SectorDimKey'
  )
}}

WITH sectors AS (
    SELECT
        SectorName,
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
            'America/New_York'
        )                      AS FileTimestamp,
        CURRENT_TIMESTAMP()     AS DBTLoadedAtStaging
    FROM {{ ref('stg_fmp__sectors') }}
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['SectorName']) }} AS SectorDimKey,
    SectorName,
    FileTimestamp,
    DBTLoadedAtStaging
FROM sectors

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
