{{
  config(
    materialized = 'incremental'
  )
}}

WITH stg AS (
    SELECT *
    FROM {{ ref('stg_fmp__cash_flow') }}   
)

SELECT
    Ticker,
    ReportDate,
    Period,
    CalendarYear,
    ReportedCurrency,
    CIK,

    NetIncome,
    DepreciationAndAmortization,
    DeferredIncomeTax,
    StockBasedCompensation,
    ChangeInWorkingCapital,
    AccountsReceivables,
    Inventory,
    AccountsPayables,
    OtherWorkingCapital,
    OtherNonCashItems,
    NetCashFromOperations,

    CapexPPE,
    AcquisitionsNet,
    PurchasesOfInvestments,
    SalesMaturitiesOfInvestments,
    OtherInvestingActivities,
    NetCashInvesting,

    DebtRepayment,
    CommonStockIssued,
    CommonStockRepurchased,
    DividendsPaid,
    OtherFinancingActivities,
    NetCashFinancing,

    EffectOfForexChangesOnCash,
    NetChangeInCash,
    CashAtEndOfPeriod,
    CashAtBeginningOfPeriod,
    OperatingCashFlow,
    CapitalExpenditure,
    FreeCashFlow,

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
