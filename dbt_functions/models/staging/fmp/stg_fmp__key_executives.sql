{{ 
    config(
        materialized = 'ephemeral'
    ) 
}}

WITH src AS (
    SELECT
        *,
        _FILE_NAME AS source_gcs_file_path       
    FROM {{ source('fmp_external_source', 'stg_fmp__key_executives_ext') }}
)

SELECT
    REGEXP_EXTRACT(source_gcs_file_path, r'_(?<ticker>[A-Za-z0-9]+)\.csv$')
                                                AS Ticker,

    CAST(title        AS STRING)                AS ExecTitle,
    CAST(name         AS STRING)                AS ExecName,
    {{ to_float('pay') }}                       AS ExecPay,
    CAST(currencyPay  AS STRING)                AS PayCurrency,
    CAST(gender       AS STRING)                AS Gender,
    {{ to_int('yearBorn') }}                    AS YearBorn,
    {{ to_int('titleSince') }}                  AS TitleSince,

    PARSE_TIMESTAMP(
        '%Y%m%d_%H%M%S',
        REGEXP_EXTRACT(source_gcs_file_path, r'(\d{8}_\d{6})'),
        'America/New_York'
    )                                           AS FileTimestamp,
    CURRENT_TIMESTAMP()                         AS DBTLoadedAtStaging
FROM src
