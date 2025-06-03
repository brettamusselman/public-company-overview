{{
  config(
    materialized='ephemeral'
  )
}}

WITH source_data AS (

    SELECT
        *,
        _FILE_NAME           AS source_gcs_file_path  
    FROM {{ source('fmp_external_source', 'stg_fmp__balance_sheet_ext') }}

)

SELECT
    CAST(symbol                AS STRING)   AS Ticker,
    CAST(reportedCurrency      AS STRING)   AS ReportedCurrency,
    CAST(cik                   AS INT64)    AS CIK,

    PARSE_DATE('%Y-%m-%d', `date`)            AS ReportDate,       
    PARSE_DATE('%Y-%m-%d', fillingDate)     AS FilingDate,
    PARSE_DATETIME('%Y-%m-%d %H:%M', acceptedDate)
                                            AS AcceptedDate,

    CAST(calendarYear          AS INT64)    AS CalendarYear,
    CAST(`period`                AS STRING)   AS Period,         

    {{ to_int('cashAndCashEquivalents')        }} AS CashAndCashEquivalents,
    {{ to_int('shortTermInvestments')          }} AS ShortTermInvestments,
    {{ to_int('cashAndShortTermInvestments')   }} AS CashAndShortTermInvestments,
    {{ to_int('netReceivables')                }} AS NetReceivables,
    {{ to_int('inventory')                     }} AS Inventory,
    {{ to_int('otherCurrentAssets')            }} AS OtherCurrentAssets,
    {{ to_int('totalCurrentAssets')            }} AS TotalCurrentAssets,

    {{ to_int('propertyPlantEquipmentNet')     }} AS PPENet,
    {{ to_int('goodwill')                      }} AS Goodwill,
    {{ to_int('intangibleAssets')              }} AS IntangibleAssets,
    {{ to_int('goodwillAndIntangibleAssets')   }} AS GoodwillAndIntangibleAssets,
    {{ to_int('longTermInvestments')           }} AS LongTermInvestments,
    {{ to_int('taxAssets')                     }} AS TaxAssets,
    {{ to_int('otherNonCurrentAssets')         }} AS OtherNonCurrentAssets,
    {{ to_int('totalNonCurrentAssets')         }} AS TotalNonCurrentAssets,
    {{ to_int('otherAssets')                   }} AS OtherAssets,

    {{ to_int('accountPayables')               }} AS AccountPayables,
    {{ to_int('shortTermDebt')                 }} AS ShortTermDebt,
    {{ to_int('taxPayables')                   }} AS TaxPayables,
    {{ to_int('deferredRevenue')               }} AS DeferredRevenue,
    {{ to_int('otherCurrentLiabilities')       }} AS OtherCurrentLiabilities,
    {{ to_int('totalCurrentLiabilities')       }} AS TotalCurrentLiabilities,
    {{ to_int('longTermDebt')                  }} AS LongTermDebt,
    {{ to_int('deferredRevenueNonCurrent')     }} AS DeferredRevenueNonCurrent,
    {{ to_int('deferredTaxLiabilitiesNonCurrent') }}
                                                AS DeferredTaxLiabilitiesNonCurrent,
    {{ to_int('otherNonCurrentLiabilities')    }} AS OtherNonCurrentLiabilities,
    {{ to_int('totalNonCurrentLiabilities')    }} AS TotalNonCurrentLiabilities,
    {{ to_int('otherLiabilities')              }} AS OtherLiabilities,
    {{ to_int('capitalLeaseObligations')       }} AS CapitalLeaseObligations,
    {{ to_int('totalLiabilities')              }} AS TotalLiabilities,

    {{ to_int('preferredStock')                }} AS PreferredStock,
    {{ to_int('commonStock')                   }} AS CommonStock,
    {{ to_int('retainedEarnings')              }} AS RetainedEarnings,
    {{ to_int('accumulatedOtherComprehensiveIncomeLoss') }}
                                                AS AOCI,
    {{ to_int('othertotalStockholdersEquity')  }} AS OtherSHEquity,
    {{ to_int('totalStockholdersEquity')       }} AS TotalStockholdersEquity,
    {{ to_int('totalEquity')                   }} AS TotalEquity,

    {{ to_int('totalAssets')                           }} AS TotalAssets,
    {{ to_int('totalLiabilitiesAndStockholdersEquity') }} AS TotalLiabilitiesAndStockholdersEquity,
    {{ to_int('minorityInterest')                      }} AS MinorityInterest,
    {{ to_int('totalLiabilitiesAndTotalEquity')        }} AS TotalLiabilitiesAndTotalEquity,
    {{ to_int('totalInvestments')                      }} AS TotalInvestments,
    {{ to_int('totalDebt')                             }} AS TotalDebt,
    {{ to_int('netDebt')                               }} AS NetDebt,

    CAST(link      AS STRING) AS FilingIndexLink,
    CAST(finalLink AS STRING) AS FilingDocumentLink,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,

    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

FROM source_data
