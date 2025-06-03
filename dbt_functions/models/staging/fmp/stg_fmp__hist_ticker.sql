{{
  config(
    materialized='ephemeral'
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__hist_ticker_daily') }}
)

SELECT
    CAST(symbol AS STRING) AS Ticker,
    CAST(`close` AS FLOAT64) AS `Close`,
    CAST(high AS FLOAT64) AS High,
    CAST(low AS FLOAT64) AS Low,
    CAST(`open` AS FLOAT64) AS `Open`,
    CAST(volume AS INT64) AS Volume,
    CAST(vwap AS FLOAT64) AS Vwap,
    CAST(change AS FLOAT64) AS Change,
    CAST(changePercent AS FLOAT64) AS ChangePercent,
    PARSE_DATETIME('%Y-%m-%d', `date`) AS EventDateTime,
    '1d' AS Inter,  

    -- Metdata from GCS
    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,
    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
FROM 
    source_data