{{ config(materialized = 'ephemeral') }}

WITH src AS (
    SELECT
        _FILE_NAME          AS source_gcs_file_path,
        TRIM(sector)        AS SectorNameRaw
    FROM {{ source('fmp_external_source', 'stg_fmp__sectors_ext') }}
)

SELECT
    INITCAP(SectorNameRaw) AS SectorName,      
    source_gcs_file_path
FROM src
