{{
  config(
    materialized='ephemeral'
  )
}}

WITH source_data AS (
    SELECT *,
    _FILE_NAME as source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__cash_flow_ext') }}
)

SELECT
    CAST(symbol AS STRING)                          AS Ticker,
    CAST(reportedCurrency AS STRING)                AS ReportedCurrency,
    {{ to_int('cik') }}                             AS CIK,

    PARSE_DATE('%Y-%m-%d',  `date`)                 AS ReportDate,
    PARSE_DATE('%Y-%m-%d',  fillingDate)            AS FilingDate,
    PARSE_DATETIME('%Y-%m-%d %H:%M', acceptedDate)  AS AcceptedDate,

    {{ to_int('calendarYear') }}                    AS CalendarYear,
    CAST(period AS STRING)                          AS Period,

    {{ to_int('netIncome') }}                       AS NetIncome,
    {{ to_int('depreciationAndAmortization') }}     AS DepreciationAndAmortization,
    {{ to_int('deferredIncomeTax') }}               AS DeferredIncomeTax,
    {{ to_int('stockBasedCompensation') }}          AS StockBasedCompensation,
    {{ to_int('changeInWorkingCapital') }}          AS ChangeInWorkingCapital,
    {{ to_int('accountsReceivables') }}             AS AccountsReceivables,
    {{ to_int('inventory') }}                       AS Inventory,
    {{ to_int('accountsPayables') }}                AS AccountsPayables,
    {{ to_int('otherWorkingCapital') }}             AS OtherWorkingCapital,
    {{ to_int('otherNonCashItems') }}               AS OtherNonCashItems,
    {{ to_int('netCashProvidedByOperatingActivities') }}
                                                   AS NetCashFromOperations,

    {{ to_int('investmentsInPropertyPlantAndEquipment') }}
                                                   AS CapexPPE,
    {{ to_int('acquisitionsNet') }}                 AS AcquisitionsNet,
    {{ to_int('purchasesOfInvestments') }}          AS PurchasesOfInvestments,
    {{ to_int('salesMaturitiesOfInvestments') }}    AS SalesMaturitiesOfInvestments,
    {{ to_int('otherInvestingActivites') }}         AS OtherInvestingActivities,
    {{ to_int('netCashUsedForInvestingActivites') }} AS NetCashInvesting,

    {{ to_int('debtRepayment') }}                   AS DebtRepayment,
    {{ to_int('commonStockIssued') }}               AS CommonStockIssued,
    {{ to_int('commonStockRepurchased') }}          AS CommonStockRepurchased,
    {{ to_int('dividendsPaid') }}                   AS DividendsPaid,
    {{ to_int('otherFinancingActivites') }}         AS OtherFinancingActivities,
    {{ to_int('netCashUsedProvidedByFinancingActivities') }}
                                                   AS NetCashFinancing,

    {{ to_int('effectOfForexChangesOnCash') }}      AS EffectOfForexChangesOnCash,
    {{ to_int('netChangeInCash') }}                 AS NetChangeInCash,
    {{ to_int('cashAtEndOfPeriod') }}               AS CashAtEndOfPeriod,
    {{ to_int('cashAtBeginningOfPeriod') }}         AS CashAtBeginningOfPeriod,
    {{ to_int('operatingCashFlow') }}               AS OperatingCashFlow,
    {{ to_int('capitalExpenditure') }}              AS CapitalExpenditure,
    {{ to_int('freeCashFlow') }}                    AS FreeCashFlow,

    CAST(link      AS STRING)                       AS FilingIndexLink,
    CAST(finalLink AS STRING)                       AS FilingDocumentLink,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                               AS FileTimestamp,
    CURRENT_TIMESTAMP()                             AS DBTLoadedAtStaging
FROM source_data
