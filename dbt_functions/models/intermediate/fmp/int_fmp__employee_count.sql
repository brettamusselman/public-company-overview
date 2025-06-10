{{
  config(
    materialized = 'incremental',
  )
}}

SELECT
    Ticker,
    CIK,
    PeriodOfReport,
    FilingDate,
    EmployeeCount,
    FormType,
    SourceURL,
    FileTimestamp,
    DBTLoadedAtStaging
FROM {{ ref('stg_fmp__employee_count') }}

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
