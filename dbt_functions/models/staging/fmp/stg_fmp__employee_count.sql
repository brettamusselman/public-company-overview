{{
  config(
    materialized='ephemeral'
  )
}}

WITH src AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path              
    FROM {{ source('fmp_external_source', 'stg_fmp__employee_count_ext') }}
)

SELECT
    CAST(symbol AS STRING)                       AS Ticker,

    SAFE_CAST(cik AS INT64)                      AS CIK,
    PARSE_DATETIME('%Y-%m-%d %H:%M', acceptanceTime)
                                                 AS AcceptanceDateTime,
    PARSE_DATE('%Y-%m-%d', periodOfReport)       AS PeriodOfReport,
    CAST(companyName AS STRING)                  AS CompanyName,
    CAST(formType    AS STRING)                  AS FormType,
    PARSE_DATE('%Y-%m-%d', filingDate)           AS FilingDate,
    {{ to_int('employeeCount') }}                AS EmployeeCount,
    CAST(source AS STRING)                       AS SourceURL,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                            AS FileTimestamp,
    CURRENT_TIMESTAMP()                          AS DBTLoadedAtStaging
FROM src
