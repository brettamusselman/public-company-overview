{{ config(materialized = 'ephemeral') }}

WITH src AS (
    SELECT
        json_raw,
        _FILE_NAME AS source_gcs_file_path       
    FROM {{ source('microlink_external_source', 'stg_microlink__text_ext') }}
)

SELECT
    CONCAT(
      'https://',
      REGEXP_REPLACE(
        REGEXP_EXTRACT(source_gcs_file_path, r'_https_(.*?)_Name'), 
        r'_',                                       
        '.'
      )
    )                                            AS WebsiteURL,

    JSON_VALUE(json_raw, '$.data.title')         AS Title,
    JSON_VALUE(json_raw, '$.data.description')   AS Description,
    JSON_VALUE(json_raw, '$.data.publisher')     AS Publisher,
    JSON_VALUE(json_raw, '$.data.date')          AS PublishDateISO,

    source_gcs_file_path                         AS JsonFilePath,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,

    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
FROM src
