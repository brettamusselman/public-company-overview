{{
  config(
    materialized='ephemeral'
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__tickers_ext') }}
)

SELECT
    CAST(symbol AS STRING) AS Ticker,
    CAST(`name` AS STRING) AS CompanyName,
    CAST(exchange AS STRING) AS ExchangeName,
    CAST(exchangeShortName AS STRING) AS ExchangeAcronym,
    CAST(`type` AS STRING) AS ExchangeType,

    -- Metdata from GCS
    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,
    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
FROM 
    source_data