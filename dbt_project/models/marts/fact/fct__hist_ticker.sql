{{
  config(
    materialized='table',
    unique_key=['Ticker', 'IntervalDimKey', 'DateDimKey', 'TimeDimKey', 'RecencyRank'],
    cluster_by=['Ticker', 'IntervalDimKey', 'DateDimKey', 'TimeDimKey']
  )
}}

WITH source_fmp AS (
    SELECT
        `Close`,
        High,
        Low,
        `Open`,
        Volume,
        Ticker,  
        EventDateTime,      
        Vwap,
        NULL AS Dividends,
        NULL AS StockSplits,
        NULL AS NumTransactions,
        Change,
        ChangePercent,
        Inter,
        FileTimestamp,
        DBTLoadedAtStaging,
        'FMP' AS DataSource
    FROM
        {{ ref('int_fmp__hist_ticker') }}
),

source_polygon AS (
    SELECT
        `Close`,
        High,
        Low,
        `Open`,
        Volume,
        Ticker,
        EventDateTime,     
        Vwap,
        NULL AS Dividends,
        NULL AS StockSplits,
        NumTransactions,
        NULL AS Change,
        NULL AS ChangePercent,
        Inter,           
        FileTimestamp,
        DBTLoadedAtStaging,
        'Polygon' AS DataSource
    FROM
        {{ ref('int_polygon__hist_ticker') }}
),

source_yfinance AS (
    SELECT
        `Close`,
        High,
        Low,
        `Open`,
        Volume,
        Ticker,
        EventDateTime,
        NULL AS Vwap,        
        Dividends,
        StockSplits,
        NULL AS NumTransactions,
        NULL AS Change,
        NULL AS ChangePercent,
        Inter,          
        FileTimestamp,
        DBTLoadedAtStaging,
        'YFinance' AS DataSource
    FROM
        {{ ref('int_yfinance__hist_ticker') }}
),

all_sources_combined AS (
    SELECT * FROM source_fmp
    UNION ALL
    SELECT * FROM source_polygon
    UNION ALL
    SELECT * FROM source_yfinance
),

dim_date_lookup AS (
    SELECT
        DateDimKey, 
        EventDate  
    FROM {{ ref('dim__date') }}
),

dim_time_lookup AS (
    SELECT
        TimeDimKey,  
        EventTime 
    FROM {{ ref('dim__time') }}
),

dim_interval_lookup AS (
    SELECT
        IntervalDimKey,  
        IntervalValue     
    FROM {{ ref('dim__interval') }} 
),

joined_with_dimensions AS (
    SELECT
        asc_data.*,
        ddl.DateDimKey,
        dtl.TimeDimKey,
        dil.IntervalDimKey
    FROM all_sources_combined asc_data
    LEFT JOIN dim_date_lookup ddl
        ON CAST(asc_data.EventDateTime AS DATE) = ddl.EventDate
    LEFT JOIN dim_time_lookup dtl
        ON CAST(asc_data.EventDateTime AS TIME) = dtl.EventTime
    LEFT JOIN dim_interval_lookup dil
        ON asc_data.Inter = dil.IntervalValue
),

ranked_combined_data AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY Ticker, DateDimKey, TimeDimKey, IntervalDimKey
            ORDER BY FileTimestamp DESC
        ) as RecencyRank
    FROM
        joined_with_dimensions
)

SELECT
    Ticker,
    IntervalDimKey,
    DateDimKey,
    TimeDimKey,
    `Open`,
    High,
    Low,
    `Close`,
    Volume,
    Vwap,
    NumTransactions,
    Dividends,
    StockSplits,
    Change,
    ChangePercent,
    FileTimestamp,    
    DBTLoadedAtStaging, 
    DataSource,         
    RecencyRank         
FROM
    ranked_combined_data
WHERE
    RecencyRank = 1