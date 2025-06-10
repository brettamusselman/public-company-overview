{{
  config(
    materialized='ephemeral'
  )
}}

WITH src AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path
    FROM {{ source('fmp_external_source', 'stg_fmp__exec_comp_ext') }}
)

SELECT
    CAST(symbol AS STRING)                      AS Ticker,

    SAFE_CAST(cik AS INT64)                     AS CIK,
    CAST(companyName AS STRING)                 AS CompanyName,
    PARSE_DATE('%m/%d/%Y', filingDate)          AS FilingDate,
    PARSE_DATETIME('%m/%d/%Y %H:%M', acceptedDate)
                                                AS AcceptedDateTime,

    CAST(nameAndPosition AS STRING)             AS ExecNameAndPosition,
    {{ to_int('year') }}                        AS CompYear,

    {{ to_float('salary') }}                    AS Salary,
    {{ to_float('bonus') }}                     AS Bonus,
    {{ to_float('stockAward') }}                AS StockAward,
    {{ to_float('optionAward') }}               AS OptionAward,
    {{ to_float('incentivePlanCompensation') }} AS IncentivePlanComp,
    {{ to_float('allOtherCompensation') }}      AS AllOtherComp,
    {{ to_float('total') }}                     AS TotalComp,

    CAST(link AS STRING)                        AS SourceURL,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                           AS FileTimestamp,
    CURRENT_TIMESTAMP()                         AS DBTLoadedAtStaging
FROM src
