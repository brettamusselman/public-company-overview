{{
  config(
    materialized='ephemeral'
  )
}}

WITH src AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path          
    FROM {{ source('fmp_external_source', 'stg_fmp__company_notes_ext') }}
)

SELECT
    CAST(symbol  AS STRING)                   AS Ticker,
    SAFE_CAST(cik AS INT64)                   AS CIK,
    CAST(title   AS STRING)                   AS NoteTitle,
    CAST(exchange AS STRING)                  AS NoteExchange,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                         AS FileTimestamp,
    CURRENT_TIMESTAMP()                       AS DBTLoadedAtStaging
FROM src
