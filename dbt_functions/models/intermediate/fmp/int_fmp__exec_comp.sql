{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    Ticker,
    CIK,
    CompYear,
    ExecNameAndPosition,
    Salary,
    Bonus,
    StockAward,
    OptionAward,
    IncentivePlanComp,
    AllOtherComp,
    TotalComp,
    SourceURL,
    FileTimestamp,
    DBTLoadedAtStaging
FROM {{ ref('stg_fmp__exec_comp') }}

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
