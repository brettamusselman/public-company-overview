{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'DateDimKey', 'Period', 'RecencyRank'],
    cluster_by   = ['TickerDimKey', 'DateDimKey']
  )
}}

WITH source_fmp AS (
    SELECT
        *,                
        'FMP' AS DataSource
    FROM {{ ref('int_fmp__cash_flow') }}
),

all_sources AS (
    SELECT * FROM source_fmp
),

dim_date AS (
    SELECT DateDimKey, EventDate
    FROM {{ ref('dim__date') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        s.*,
        d.DateDimKey,
        t.TickerDimKey
    FROM all_sources s
    LEFT JOIN dim_date   d ON s.ReportDate = d.EventDate
    LEFT JOIN dim_ticker t ON s.Ticker     = t.Ticker
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey, DateDimKey, Period
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    TickerDimKey,
    DateDimKey,
    Period,
    CalendarYear,

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
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank

FROM ranked
WHERE RecencyRank = 1
