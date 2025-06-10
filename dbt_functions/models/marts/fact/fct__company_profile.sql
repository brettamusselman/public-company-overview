{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'RecencyRank'],
    cluster_by   = ['TickerDimKey']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__company_profile') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

dim_exchange AS (
    SELECT ExchangeDimKey, ExchangeAcronym, SourceSystem
    FROM {{ ref('dim__exchanges') }}
),

dim_sector AS (
    SELECT SectorDimKey, SectorName
    FROM {{ ref('dim__sectors') }}
),

dim_industry AS (
    SELECT IndustryDimKey, IndustryName
    FROM {{ ref('dim__industries') }}
),

dim_country AS (
    SELECT CountryDimKey, CountryCode
    FROM {{ ref('dim__countries') }}
),

joined AS (
    SELECT
        t.TickerDimKey,
        e.ExchangeDimKey,
        sec.SectorDimKey,
        ind.IndustryDimKey,
        c.CountryDimKey,

        s.* EXCEPT (
            Ticker,
            ExchangeShortName,
            Sector,
            Industry,
            Country
        )
    FROM src s
    LEFT JOIN dim_ticker   t  ON s.Ticker             = t.Ticker
    LEFT JOIN dim_exchange e  ON s.ExchangeShortName  = e.ExchangeAcronym
                              AND e.SourceSystem      = s.DataSource
    LEFT JOIN dim_sector   sec ON LOWER(s.Sector)     = LOWER(sec.SectorName)
    LEFT JOIN dim_industry ind ON LOWER(s.Industry)   = LOWER(ind.IndustryName)
    LEFT JOIN dim_country  c   ON UPPER(s.Country)    = UPPER(c.CountryCode)
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    *
FROM ranked
WHERE RecencyRank = 1
