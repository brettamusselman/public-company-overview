{{
  config(
    materialized='incremental',
    unique_key='ExchangeDimKey'
  )
}}

WITH exchanges AS (
    SELECT
        *
    FROM {{ ref('stg_polygon__exchanges') }}
    WHERE AssetClass='stocks' AND MIC IS NOT NULL
),

exchanges_with_effective_mic AS (
    SELECT
        *,
        COALESCE(MIC, OperatingMIC) AS EffectiveMIC
    FROM exchanges
),

enriched_exchange_details AS (
    SELECT
        ex.EffectiveMIC AS ExchangeDimKey,
        COALESCE(ex.ExchangeAcronym, iso_details.ACRONYM) AS ExchangeAcronym,
        ex.EffectiveMIC AS MIC,
        ex.OperatingMIC AS OperatingMIC,
        iso_details.OPRT_SGMT AS OperatingOrSegment,
        iso_details.MARKET_NAME_INSTITUTION_DESCRIPTION AS ExchangeDescription,
        iso_details.ISO_COUNTRY_CODE AS ISOCountryCode,
        COALESCE(iso_details.WEBSITE, ex.Webiste) AS Website,
        ex.FileTimestamp,
        ex.DBTLoadedAtStaging
    FROM exchanges_with_effective_mic ex
    LEFT JOIN {{ ref('seed_mic_iso') }} iso_details
        ON ex.EffectiveMIC = iso_details.MIC
)

SELECT
    *
FROM enriched_exchange_details
{% if is_incremental() %}
WHERE FileTimestamp > (SELECT coalesce(max(FileTimestamp), PARSE_TIMESTAMP('%Y-%m-%d', '{{ var('past_proof_date') }}')) FROM {{ this }} )
{% endif %}