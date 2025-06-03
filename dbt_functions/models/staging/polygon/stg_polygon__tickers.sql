{{
  config(
    materialized='ephemeral'
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('polygon_external_source', 'stg_polygon__tickers_ext') }}
)

SELECT
    CAST(ticker AS STRING) AS Ticker,
    CAST(`name` AS STRING) AS CompanyName,
    CAST(market AS STRING) AS Market,
    CAST(locale AS STRING) AS Locale,
    CAST(primary_exchange AS STRING) AS PrimaryExchange,
    CAST(`type` AS STRING) AS TickerType,
    CAST(active AS BOOLEAN) AS Active,
    CAST(currency_name AS STRING) AS CurrencyName,
    CAST(cik AS INT64) AS CIK,
    CAST(composite_figi AS STRING) AS CompositeFigi,
    CAST(share_class_figi AS STRING) AS ShareClassFigi,
    PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', last_updated_utc) AS LastUpdatedUtc,
    CAST(currency_symbol AS STRING) AS CurrencySymbol,
    CAST(base_currency_symbol AS STRING) AS BaseCurrencySymbol,
    CAST(base_currency_name AS STRING) AS BaseCurrencyName,
    CAST(source_feed AS STRING) AS SourceFeed,

    -- Metdata from GCS
    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,
    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
FROM 
    source_data