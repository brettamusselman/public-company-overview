{{
  config(
    materialized='incremental',
    unique_key=['Ticker', 'Datetime'],
    cluster_by=['Ticker'],
    partition_by={
      "field": "Datetime",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('polygon_external_source', 'stg_polygon__hist_ticker') }}
),

renamed_and_casted AS (
    SELECT
        CAST(`Close` AS FLOAT64) AS `Close`,
        CAST(High AS FLOAT64) AS High,
        CAST(Low AS FLOAT64) AS Low,
        CAST(`Open` AS FLOAT64) AS `Open`,
        CAST(CAST(Volume AS FLOAT64) AS INT64) AS Volume,
        CAST(REGEXP_EXTRACT(source_gcs_file_path, r'_([^_]+)\.csv$') AS STRING) AS Ticker,
        CAST(Vwap AS FLOAT64) AS Vwap,
        CAST(Num_Transactions AS INT64) AS NumTransactions,
        TIMESTAMP_MILLIS(CAST(`Timestamp` AS INT64)) AS `Datetime`,

        -- Metdata from GCS
        PARSE_TIMESTAMP(
            '%Y%m%d_%H%M%S',
            REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})')
        ) AS FileTimestamp,
        CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

    FROM source_data
)

SELECT * FROM renamed_and_casted
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}