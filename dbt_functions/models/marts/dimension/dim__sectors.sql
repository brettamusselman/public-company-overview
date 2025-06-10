{{
  config(
    materialized = 'table',
    unique_key   = 'SectorDimKey'
  )
}}

WITH fmp AS (
    SELECT
        SectorDimKey,
        SectorName,
        FileTimestamp,
        DBTLoadedAtStaging,
        'FMP' AS SourceSystem,
        1     AS source_priority
    FROM {{ ref('dim_fmp__sectors') }}
),

all_feeds AS (
    SELECT * FROM fmp
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY SectorDimKey
            ORDER BY source_priority ASC, FileTimestamp DESC
        ) AS rn
    FROM all_feeds
)

SELECT
    SectorDimKey,
    SectorName,
    FileTimestamp,
    DBTLoadedAtStaging,
    SourceSystem,
    rn AS Rank
FROM ranked
WHERE rn = 1
