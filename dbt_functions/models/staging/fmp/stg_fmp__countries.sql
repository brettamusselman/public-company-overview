{{ config(materialized = 'ephemeral') }}

SELECT
    UPPER(TRIM(country))           AS CountryCode,
    _FILE_NAME                     AS source_gcs_file_path
FROM {{ source('fmp_external_source', 'stg_fmp__countries_ext') }}
