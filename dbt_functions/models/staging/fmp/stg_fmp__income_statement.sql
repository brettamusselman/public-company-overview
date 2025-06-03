{{ 
    config(
        materialized = 'ephemeral'
    ) 
}}

WITH source_data AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__income_statement_ext') }}
)

SELECT
    CAST(symbol           AS STRING)             AS Ticker,
    CAST(reportedCurrency AS STRING)             AS ReportedCurrency,
    {{ to_int('cik') }}                          AS CIK,

    PARSE_DATE('%Y-%m-%d', `date`)                 AS ReportDate,
    PARSE_DATE('%Y-%m-%d', fillingDate)          AS FilingDate,
    PARSE_DATETIME('%Y-%m-%d %H:%M', acceptedDate)
                                                AS AcceptedDate,

    {{ to_int('calendarYear') }}                 AS CalendarYear,
    CAST(`period` AS STRING)                       AS Period,

    {{ to_int('revenue') }}                      AS Revenue,
    {{ to_int('costOfRevenue') }}                AS CostOfRevenue,
    {{ to_int('grossProfit') }}                  AS GrossProfit,
    SAFE_CAST(grossProfitRatio AS FLOAT64)       AS GrossProfitRatio,

    {{ to_int('researchAndDevelopmentExpenses') }}        AS RnDExpenses,
    {{ to_int('generalAndAdministrativeExpenses') }}      AS GAndAExpenses,
    {{ to_int('sellingAndMarketingExpenses') }}           AS SellingAndMarketingExpenses,
    {{ to_int('sellingGeneralAndAdministrativeExpenses') }} AS SGnAExpenses,
    {{ to_int('otherExpenses') }}               AS OtherExpenses,
    {{ to_int('operatingExpenses') }}           AS OperatingExpenses,
    {{ to_int('costAndExpenses') }}             AS CostAndExpenses,

    {{ to_int('interestIncome') }}              AS InterestIncome,
    {{ to_int('interestExpense') }}             AS InterestExpense,
    {{ to_int('depreciationAndAmortization') }} AS DepreciationAndAmortization,

    {{ to_int('ebitda') }}                      AS EBITDA,
    SAFE_CAST(ebitdaratio AS FLOAT64)           AS EBITDARatio,
    {{ to_int('operatingIncome') }}             AS OperatingIncome,
    SAFE_CAST(operatingIncomeRatio AS FLOAT64)  AS OperatingIncomeRatio,
    {{ to_int('totalOtherIncomeExpensesNet') }} AS TotalOtherIncomeExpensesNet,
    {{ to_int('incomeBeforeTax') }}             AS IncomeBeforeTax,
    SAFE_CAST(incomeBeforeTaxRatio AS FLOAT64)  AS IncomeBeforeTaxRatio,
    {{ to_int('incomeTaxExpense') }}            AS IncomeTaxExpense,
    {{ to_int('netIncome') }}                   AS NetIncome,
    SAFE_CAST(netIncomeRatio AS FLOAT64)        AS NetIncomeRatio,

    SAFE_CAST(eps AS FLOAT64)                   AS EPS,
    SAFE_CAST(epsdiluted AS FLOAT64)            AS EPSDiluted,
    {{ to_int('weightedAverageShsOut') }}       AS WeightedAverageShares,
    {{ to_int('weightedAverageShsOutDil') }}    AS WeightedAverageSharesDiluted,

    CAST(link      AS STRING) AS FilingIndexLink,
    CAST(finalLink AS STRING) AS FilingDocumentLink,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    ) AS FileTimestamp,

    CURRENT_TIMESTAMP() AS DBTLoadedAtStaging

FROM source_data
