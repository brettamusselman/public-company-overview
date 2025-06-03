{{
  config(
    materialized = 'incremental',
  )
}}

WITH balance_sheet AS (

    SELECT *
    FROM {{ ref('stg_fmp__balance_sheet') }}

)

SELECT
    -- identifiers & dates
    Ticker,
    ReportDate,                    -- DATE
    `Period`,                        -- FY / Q1 / Q2 …
    CalendarYear,                  -- INT64

    -- assets
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

    -- liabilities
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

    -- equity
    PreferredStock,
    CommonStock,
    RetainedEarnings,
    AOCI,
    OtherSHEquity,
    TotalStockholdersEquity,
    TotalEquity,

    -- totals
    TotalLiabilitiesAndStockholdersEquity,
    MinorityInterest,
    TotalLiabilitiesAndTotalEquity,
    TotalInvestments,
    TotalDebt,
    NetDebt,

    -- links & metadata
    FilingIndexLink,
    FilingDocumentLink,
    FileTimestamp,
    DBTLoadedAtStaging

FROM balance_sheet

{% if is_incremental() %}
WHERE
    FileTimestamp >
      (
        SELECT COALESCE(
                 MAX(FileTimestamp),
                 PARSE_TIMESTAMP('%Y-%m-%d', '{{ var("past_proof_date") }}')
               )
        FROM {{ this }}
      )
{% endif %}
