{{
  config(
    materialized='ephemeral'
  )
}}

WITH src AS (
    SELECT *, _FILE_NAME AS source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__company_profile_ext') }}
)

SELECT
    CAST(symbol AS STRING)                       AS Ticker,
    {{ to_float('price') }}                      AS Price,
    {{ to_float('beta') }}                       AS Beta,
    {{ to_int('volAvg') }}                       AS VolumeAvg,
    {{ to_int('mktCap') }}                       AS MarketCap,
    {{ to_float('lastDiv') }}                    AS LastDividend,
    CAST(rng AS STRING)                          AS FiftyTwoWeekRange,
    {{ to_float('changes') }}                    AS PriceChange,

    CAST(companyName AS STRING)                  AS CompanyName,
    CAST(currency     AS STRING)                 AS Currency,
    {{ to_int('cik') }}                          AS CIK,
    CAST(isin AS STRING)                         AS ISIN,
    CAST(cusip AS STRING)                        AS CUSIP,
    CAST(exchange AS STRING)                     AS Exchange,
    CAST(exchangeShortName AS STRING)            AS ExchangeShortName,
    CAST(industry AS STRING)                     AS Industry,
    CAST(website  AS STRING)                     AS Website,
    CAST(description AS STRING)                  AS Description,
    CAST(ceo AS STRING)                          AS CEO,
    CAST(sector AS STRING)                       AS Sector,
    CAST(country AS STRING)                      AS Country,
    {{ to_int('fullTimeEmployees') }}            AS FullTimeEmployees,

    CAST(phone   AS STRING)                      AS Phone,
    CAST(address AS STRING)                      AS Address,
    CAST(city    AS STRING)                      AS City,
    CAST(state   AS STRING)                      AS State,
    CAST(zip     AS STRING)                      AS ZipCode,

    {{ to_float('dcfDiff') }}                    AS DCFDiff,
    {{ to_float('dcf') }}                        AS DCF,
    CAST(image AS STRING)                        AS LogoURL,

    PARSE_DATE('%Y-%m-%d', ipoDate)              AS IPODATE,
    {{ to_bool('defaultImage') }}                AS HasDefaultImage,
    {{ to_bool('isEtf') }}                       AS IsETF,
    {{ to_bool('isActivelyTrading') }}           AS IsActivelyTrading,
    {{ to_bool('isAdr') }}                       AS IsADR,
    {{ to_bool('isFund') }}                      AS IsFund,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                            AS FileTimestamp,
    CURRENT_TIMESTAMP()                          AS DBTLoadedAtStaging
FROM src
