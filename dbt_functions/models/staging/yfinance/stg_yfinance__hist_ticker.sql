{{
  config(
    materialized='ephemeral'
  )
}}

WITH hist_ticker_daily AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('yf_external_source', 'stg_yfinance__hist_ticker_daily') }}
),

hist_ticker_interval AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('yf_external_source', 'stg_yfinance__hist_ticker_interval') }}
),

hist_tickers_interval AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('yf_external_source', 'stg_yfinance__hist_tickers_interval') }}
),

cast_hist_ticker_daily AS (
    SELECT
        CAST(NULLIF(Ticker, '') AS STRING) AS Ticker,
        CAST(`Close` AS FLOAT64) AS `Close`,
        CAST(High AS FLOAT64) AS High,
        CAST(Low AS FLOAT64) AS Low,
        CAST(`Open` AS FLOAT64) AS `Open`,
        CAST(Volume AS INT64) AS Volume,
        NULL AS Dividends,
        NULL AS StockSplits,
        '1d' AS Inter,

        PARSE_DATETIME('%Y-%m-%d', `Date`) AS EventDateTime,

        -- Metdata from GCS
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
            'America/New_York'
        ) AS FileTimestamp,
        CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

    FROM hist_ticker_daily
),

cast_hist_ticker_interval AS (
    SELECT
        CAST(NULLIF(Ticker, '') AS STRING) AS Ticker,
        CAST(`Close` AS FLOAT64) AS `Close`,
        CAST(High AS FLOAT64) AS High,
        CAST(Low AS FLOAT64) AS Low,
        CAST(`Open` AS FLOAT64) AS `Open`,
        CAST(Volume AS INT64) AS Volume,
        CAST(Dividends AS FLOAT64) AS Dividends,
        CAST(StockSplits AS FLOAT64) AS StockSplits,
        REGEXP_EXTRACT(source_gcs_file_path, r'/interval/([^/]+)/') AS Inter,

        DATETIME(
          PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', `Date`),
          'America/New_York'
        ) AS EventDateTime,

        -- Metdata from GCS
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
            'America/New_York'
        ) AS FileTimestamp,
        CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

    FROM hist_ticker_interval
),

cast_hist_tickers_interval AS (
    SELECT
        CAST(NULLIF(Ticker, '') AS STRING) AS Ticker,
        CAST(`Close` AS FLOAT64) AS `Close`,
        CAST(High AS FLOAT64) AS High,
        CAST(Low AS FLOAT64) AS Low,
        CAST(`Open` AS FLOAT64) AS `Open`,
        CAST(Volume AS INT64) AS Volume,
        CAST(Dividends AS FLOAT64) AS Dividends,
        CAST(StockSplits AS FLOAT64) AS StockSplits,
        REGEXP_EXTRACT(source_gcs_file_path, r'/interval/([^/]+)/') AS Inter,
        
        COALESCE(
            -- CASE 1: Parse as a full timestamp with offset.
            DATETIME(
                SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S%Ez', `Date`),
                'America/New_York'
            ),
            -- CASE 2: If its not a timestamp, because YF likes to mix formats on the same endpoint
            SAFE.PARSE_DATETIME('%Y-%m-%d', `Date`)
        ) AS EventDateTime,

        -- Metdata from GCS
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
            'America/New_York'
        ) AS FileTimestamp,
        CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

    FROM hist_tickers_interval
)

SELECT * FROM cast_hist_ticker_daily
UNION ALL
SELECT * FROM cast_hist_ticker_interval
UNION ALL
SELECT * FROM cast_hist_tickers_interval