{{
  config(
    materialized='incremental',
    unique_key='ExchangeDimKey'
  )
}}

WITH exchanges AS (
    SELECT
        *
    FROM {{ ref('stg_fmp__exchanges') }}
),

mapped_to_mic AS ( -- Can't use iso's table acronym column because FMP has acronyms that don't align with it.
    SELECT
        mic_map.MIC,
        ex.*
    FROM exchanges ex
    LEFT JOIN {{ ref('seed_fmp_to_mic_mapping') }} mic_map
        ON ex.ExchangeAcronym = mic_map.ExchangeAcronym
),

enriched_exchange_details AS (
    SELECT
        mapped.MIC AS ExchangeDimKey,
        COALESCE(mapped.ExchangeAcronym, iso_details.ACRONYM) AS ExchangeAcronym,
        mapped.MIC AS MIC,
        iso_details.OPERATING_MIC AS OperatingMIC,
        iso_details.OPRT_SGMT AS OperatingOrSegment,
        iso_details.MARKET_NAME_INSTITUTION_DESCRIPTION AS ExchangeDescription,
        iso_details.ISO_COUNTRY_CODE AS ISOCountryCode,
        iso_details.WEBSITE AS Website,
        mapped.FileTimestamp,
        mapped.DBTLoadedAtStaging
    FROM mapped_to_mic mapped
    LEFT JOIN {{ ref('seed_mic_iso') }} iso_details
        ON mapped.MIC = iso_details.MIC
)

SELECT
    *
FROM enriched_exchange_details
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}