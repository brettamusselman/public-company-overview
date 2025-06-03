{{
  config(
    materialized = 'incremental',
    unique_key   = ['Ticker', 'ReportDate', 'Period']
  )
}}

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_fmp__income_statement') }} 
)

SELECT
    Ticker,
    ReportDate,
    `Period`,
    CalendarYear,
    ReportedCurrency,
    CIK,

    Revenue,
    CostOfRevenue,
    GrossProfit,
    GrossProfitRatio,

    RnDExpenses,
    GAndAExpenses,
    SellingAndMarketingExpenses,
    SGnAExpenses,
    OtherExpenses,
    OperatingExpenses,
    CostAndExpenses,

    InterestIncome,
    InterestExpense,
    DepreciationAndAmortization,

    EBITDA,
    EBITDARatio,
    OperatingIncome,
    OperatingIncomeRatio,
    TotalOtherIncomeExpensesNet,
    IncomeBeforeTax,
    IncomeBeforeTaxRatio,
    IncomeTaxExpense,
    NetIncome,
    NetIncomeRatio,

    EPS,
    EPSDiluted,
    WeightedAverageShares,
    WeightedAverageSharesDiluted,

    FilingIndexLink,
    FilingDocumentLink,

    FileTimestamp,
    DBTLoadedAtStaging

FROM stg

{% if is_incremental() %}
WHERE FileTimestamp >
      (
        SELECT COALESCE(
                 MAX(FileTimestamp),
                 PARSE_TIMESTAMP('%Y-%m-%d', '{{ var("past_proof_date") }}')
               )
        FROM {{ this }}
      )
{% endif %}
