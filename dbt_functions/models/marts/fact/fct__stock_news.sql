{{
  config(
    materialized = 'table',
    unique_key   = ['TickerDimKey', 'PublishedDateTime', 'ArticleURL', 'RecencyRank'],
    cluster_by   = ['TickerDimKey', 'PublishedDateTime']
  )
}}

WITH src AS (
    SELECT *, 'FMP' AS DataSource
    FROM {{ ref('int_fmp__stock_news') }}
),

dim_ticker AS (
    SELECT TickerDimKey, Ticker
    FROM {{ ref('dim__tickers') }}
),

joined AS (
    SELECT
        d.TickerDimKey,
        s.PublishedDateTime,
        s.Publisher,
        s.Title,
        s.ImageURL,
        s.Site,
        s.ArticleText,
        s.ArticleURL,
        s.FileTimestamp,
        s.DBTLoadedAtStaging,
        s.DataSource
    FROM src s
    LEFT JOIN dim_ticker d
      ON s.Ticker = d.Ticker
),

ranked AS (
    SELECT
        *,
        DENSE_RANK() OVER (
            PARTITION BY TickerDimKey, PublishedDateTime, ArticleURL
            ORDER BY FileTimestamp DESC
        ) AS RecencyRank
    FROM joined
)

SELECT
    TickerDimKey,
    PublishedDateTime,
    Publisher,
    Title,
    ImageURL,
    Site,
    ArticleText,
    ArticleURL,
    FileTimestamp,
    DBTLoadedAtStaging,
    DataSource,
    RecencyRank
FROM ranked
WHERE RecencyRank = 1
