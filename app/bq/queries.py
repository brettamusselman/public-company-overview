available_tickers="SELECT DISTINCT Ticker FROM `public-company-overview.pco_dataset.dim__tickers`"
available_sources="SELECT DISTINCT DataSource FROM `public-company-overview.pco_dataset.fct__hist_ticker`"
stock_data_query_template = """
SELECT
    tick.Ticker,
    PARSE_DATE('%Y%m%d', CAST(fct.DateDimKey AS STRING)) AS date,
    fct.Open,
    fct.High,
    fct.Low,
    fct.Close,
    fct.Volume,
    fct.FileTimestamp,
    fct.DBTLoadedAtStaging,
    fct.DataSource,
    dim.BaseUnit,
    dim.IntervalValue,
    dim.IntervalDescription
FROM `public-company-overview.pco_dataset.fct__hist_ticker` fct
JOIN `public-company-overview.pco_dataset.dim__tickers` tick
    ON fct.TickerDimKey = tick.TickerDimKey
JOIN `public-company-overview.pco_dataset.dim__interval` dim
    ON fct.IntervalDimKey = dim.IntervalDimKey
WHERE tick.Ticker = '{ticker}'
    AND PARSE_DATE('%Y%m%d', CAST(fct.DateDimKey AS STRING)) BETWEEN '{start_date}' AND '{end_date}'
    AND dim.BaseUnit = '{base_unit}'
ORDER BY date
"""
