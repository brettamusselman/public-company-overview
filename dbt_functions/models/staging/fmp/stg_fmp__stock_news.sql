{{ 
    config(
        materialized = 'ephemeral'
    ) 
}}

WITH src AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__stock_news_ext') }}
)

SELECT
    CAST(symbol AS STRING)                                      AS Ticker,
    PARSE_DATETIME('%Y-%m-%d %H:%M:%S', publishedDate)
                                                                AS PublishedDateTime,
    CAST(publisher AS STRING)                                   AS Publisher,
    CAST(title     AS STRING)                                   AS Title,
    CAST(image     AS STRING)                                   AS ImageURL,
    CAST(site      AS STRING)                                   AS Site,
    CAST(text      AS STRING)                                   AS ArticleText,
    CAST(url       AS STRING)                                   AS ArticleURL,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                                           AS FileTimestamp,
    CURRENT_TIMESTAMP()                                         AS DBTLoadedAtStaging
FROM src
