{{ config(materialized = 'ephemeral') }}

WITH src AS (
    SELECT
        _FILE_NAME                     AS source_gcs_file_path,
        TRIM(industry)                 AS IndustryNameRaw
    FROM {{ source('fmp_external_source', 'stg_fmp__industries_ext') }}
)

SELECT
    INITCAP(IndustryNameRaw) AS IndustryName,      
    source_gcs_file_path
FROM src
