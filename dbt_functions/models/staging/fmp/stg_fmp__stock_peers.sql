{{ 
    config(
        materialized = 'ephemeral'
    ) 
}}

WITH src AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__stock_peers_ext') }}
)

SELECT
    REGEXP_EXTRACT(
        source_gcs_file_path,
        r'_(?<ticker>[A-Za-z0-9]+)\.csv$'
    )                                       AS Ticker,
    CAST(symbol       AS STRING)            AS PeerTicker,
    CAST(companyName  AS STRING)            AS PeerCompanyName,
    SAFE_CAST(price   AS FLOAT64)           AS PeerPrice,
    {{ to_int('mktCap') }}                  AS PeerMarketCap,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,
    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging
FROM src
