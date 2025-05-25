{{
  config(
    materialized='incremental',
    cluster_by=['Ticker', 'Inter'],
    partition_by={
      "field": "Datetime",
      "data_type": "datetime",
      "granularity": "day"
    }
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path,
    REGEXP_EXTRACT(_FILE_NAME, r'interval\/([^\/]+)\/') AS raw_interval_string
    FROM {{ source('polygon_external_source', 'stg_polygon__hist_ticker_interval') }}
),

renamed_and_casted AS (
    SELECT
        CAST(REGEXP_EXTRACT(source_gcs_file_path, r'_([^_]+)\.csv$') AS STRING) AS Ticker,
        CAST(`Close` AS FLOAT64) AS `Close`,
        CAST(High AS FLOAT64) AS High,
        CAST(Low AS FLOAT64) AS Low,
        CAST(`Open` AS FLOAT64) AS `Open`,
        CAST(CAST(Volume AS FLOAT64) AS INT64) AS Volume,
        CAST(Vwap AS FLOAT64) AS Vwap,
        CAST(Num_Transactions AS INT64) AS NumTransactions,

        COALESCE(REGEXP_EXTRACT(raw_interval_string, r'^(\d+)'), '1') ||
        CASE LOWER(REGEXP_EXTRACT(raw_interval_string, r'\d*([a-zA-Z]+)$'))
            WHEN 'minute' THEN 'm'
            WHEN 'hour' THEN 'h'
            WHEN 'day' THEN 'd'
            WHEN 'week' THEN 'wk'
            WHEN 'month' THEN 'mo'
            ELSE '_unknown_unit_'
        END AS Inter,

        DATETIME(
          TIMESTAMP_MILLIS(CAST(`Timestamp` AS INT64)),
          'America/New_York'
        ) AS `Datetime`,

        -- Metdata from GCS
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
            'America/New_York'
        ) AS FileTimestamp,
        CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

    FROM source_data
)

SELECT * FROM renamed_and_casted
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}