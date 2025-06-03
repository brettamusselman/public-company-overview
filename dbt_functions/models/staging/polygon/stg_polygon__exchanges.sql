{{
  config(
    materialized='ephemeral'
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('polygon_external_source', 'stg_polygon__exchanges_ext') }}
)

SELECT
    CAST(`type` AS STRING) AS ExchangeType,
    CAST(asset_class AS STRING) AS AssetClass,
    CAST(locale AS STRING) AS Locale,
    CAST(`name` AS STRING) AS ExchangeName,
    CAST(acronym AS STRING) AS ExchangeAcronym,
    CAST(mic AS STRING) AS MIC,
    CAST(operating_mic AS STRING) AS OperatingMIC,
    CAST(participant_id AS STRING) AS ParticipantId,
    CAST(`url` AS STRING) AS Webiste,

    -- Metdata from GCS
    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,
    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
FROM 
    source_data