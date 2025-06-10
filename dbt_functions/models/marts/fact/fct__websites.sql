{{
  config(
    materialized = 'table',
    unique_key   = ['WebsiteURL', 'RecencyRank']
  )
}}

WITH src AS (
    SELECT *, 'Microlink' AS DataSource
    FROM {{ ref('int_microlink__websites') }}
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY WebsiteURL
            ORDER BY JsonFilePath DESC
        ) AS RecencyRank
    FROM src
)

SELECT
    WebsiteURL,
    Title,
    Description,
    Publisher,
    PublishDateISO,
    JsonFilePath,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
