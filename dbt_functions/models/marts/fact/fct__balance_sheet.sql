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
    FROM {{ ref('int_fmp__balance_sheet') }}
),

all_sources AS (
    SELECT * FROM source_fmp
),

dim_date AS (
    SELECT
        DateDimKey,
        EventDate
    FROM {{ ref('dim__date') }}
),

dim_ticker AS (
    SELECT
        TickerDimKey,
        Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        s.*,
        d.DateDimKey,
        t.TickerDimKey
    FROM all_sources s
    LEFT JOIN dim_date d
      ON s.ReportDate = d.EventDate
    LEFT JOIN dim_ticker t
      ON s.Ticker = t.Ticker
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey, DateDimKey, `Period`
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    -- grain columns
    TickerDimKey,
    DateDimKey,
    `Period`,
    CalendarYear,

    -- financial statement
    CashAndCashEquivalents,
    ShortTermInvestments,
    CashAndShortTermInvestments,
    NetReceivables,
    Inventory,
    OtherCurrentAssets,
    TotalCurrentAssets,
    PPENet,
    Goodwill,
    IntangibleAssets,
    GoodwillAndIntangibleAssets,
    LongTermInvestments,
    TaxAssets,
    OtherNonCurrentAssets,
    TotalNonCurrentAssets,
    OtherAssets,
    TotalAssets,

    AccountPayables,
    ShortTermDebt,
    TaxPayables,
    DeferredRevenue,
    OtherCurrentLiabilities,
    TotalCurrentLiabilities,
    LongTermDebt,
    DeferredRevenueNonCurrent,
    DeferredTaxLiabilitiesNonCurrent,
    OtherNonCurrentLiabilities,
    TotalNonCurrentLiabilities,
    OtherLiabilities,
    CapitalLeaseObligations,
    TotalLiabilities,

    PreferredStock,
    CommonStock,
    RetainedEarnings,
    AOCI,
    OtherSHEquity,
    TotalStockholdersEquity,
    TotalEquity,

    TotalLiabilitiesAndStockholdersEquity,
    MinorityInterest,
    TotalLiabilitiesAndTotalEquity,
    TotalInvestments,
    TotalDebt,
    NetDebt,

    -- lineage
    FilingIndexLink,
    FilingDocumentLink,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank

FROM ranked
WHERE RecencyRank = 1
