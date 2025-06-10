{{
  config(
    materialized = 'table',
    unique_key   = 'CountryDimKey'
  )
}}

WITH fmp_countries AS (          
    SELECT
        CountryDimKey,
        CountryCode,
        FileTimestamp,
        DBTLoadedAtStaging,
        'FMP' AS SourceSystem,
        1     AS source_priority
    FROM {{ ref('dim_fmp__countries') }}
),

all_feeds AS (
    SELECT * FROM fmp_countries
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY CountryDimKey
            ORDER BY source_priority ASC, FileTimestamp DESC
        ) AS rn
    FROM all_feeds
)

SELECT
    CountryDimKey,
    CountryCode,
    FileTimestamp,
    DBTLoadedAtStaging,
    SourceSystem,
    rn AS Rank
FROM ranked
WHERE rn = 1
