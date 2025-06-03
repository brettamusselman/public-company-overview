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
    FROM {{ ref('int_fmp__income_statement') }}
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
    FROM all_sources        s
    LEFT JOIN dim_date   d  ON s.ReportDate = d.EventDate
    LEFT JOIN dim_ticker t  ON s.Ticker     = t.Ticker
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
    `Period`,
    CalendarYear,

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
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank

FROM ranked
WHERE RecencyRank = 1
