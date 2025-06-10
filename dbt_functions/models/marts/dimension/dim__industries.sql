{{
  config(
    materialized = 'table',
    unique_key   = 'IndustryDimKey'
  )
}}

WITH fmp AS (
    SELECT
        IndustryDimKey,
        IndustryName,
        FileTimestamp,
        DBTLoadedAtStaging,
        'FMP' AS SourceSystem,
        1     AS source_priority
    FROM {{ ref('dim_fmp__industries') }}
),

all_feeds AS (
    SELECT * FROM fmp
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY IndustryDimKey
            ORDER BY source_priority ASC, FileTimestamp DESC
        ) AS rn
    FROM all_feeds
)

SELECT
    IndustryDimKey,
    IndustryName,
    FileTimestamp,
    DBTLoadedAtStaging,
    SourceSystem,
    rn AS Rank
FROM ranked
WHERE rn = 1
