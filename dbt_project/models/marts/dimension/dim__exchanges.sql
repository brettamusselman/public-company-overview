{{
  config(
    materialized='table',
    unique_key='ExchangeDimKey'
  )
}}

WITH fmp_derived_exchanges AS (
    SELECT
        *,
        'FMP' AS SourceSystem,
        1 AS source_priority
    FROM {{ ref('dim_fmp__exchanges') }}
),

polygon_derived_exchanges AS (
    SELECT
        *,
        'Polygon' AS SourceSystem,
        2 AS source_priority
    FROM {{ ref('dim_polygon__exchanges') }} 
),

all_derived_exchanges_unioned AS (
    SELECT * FROM fmp_derived_exchanges
    UNION ALL
    SELECT * FROM polygon_derived_exchanges
),

final_ranked_exchanges AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY ExchangeDimKey
            ORDER BY source_priority ASC, FileTimestamp DESC
        ) as rn
    FROM all_derived_exchanges_unioned
)

SELECT
    ExchangeDimKey,   
    ExchangeAcronym,
    MIC,              
    OperatingMIC,
    OperatingOrSegment,
    ExchangeDescription,
    ISOCountryCode,
    Website,
    FileTimestamp,      
    DBTLoadedAtStaging,     
    SourceSystem,
    rn AS Rank       
FROM final_ranked_exchanges
WHERE rn=1