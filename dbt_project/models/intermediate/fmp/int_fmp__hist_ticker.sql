{{
  config(
    materialized='incremental',
    cluster_by=['Ticker'],
    partition_by={
      "field": "Datetime",
      "data_type": "datetime",
      "granularity": "day"
    }
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__hist_ticker_daily') }}
),

renamed_and_casted AS (
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
        PARSE_DATETIME('%Y-%m-%d', `date`) AS `Datetime`,  

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