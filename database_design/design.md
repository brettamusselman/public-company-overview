For the underlying database used by the application, we can use anything such as a SQL server, sets of CSV/parquet files with a manifest document, or (more likely) a BigQuery dataset.

Source files:
- They will be stored in "pco-store" bucket on GCP Cloud Storage

Transformations/Dimensional Modeling:
- We can do this with either a set of BigQuery scripts and orchestrate through Python or through Dataflow/Dataproc
    - I am leaning towards using DBT because apparently the dbt-bigquery package is really mature

Design:
- Ingest from "pco-store" into a BigQuery dataset and add a prefix to each table with "raw_{table name}"
    - What we should really do is make sure the file drops off in a folder and just a script to read in each folder as a raw table
- Next, we want to move into the silver (intermediate) layer
    - Every table should have the prefix "int_{table name}"
    - In this layer, the main point is to move into conformed dimensional modelling for dimension and fact tables
        - Dimensions:
            - Entities (int_entities)
                - List of entities with their tickers, exchanges, names, URLs, descriptions, and source systems
        - Fact:
        - Note: all of this should be "int_{table name}_{source system}" so there can be overlapping datasets from each source system
            - Historical ticker performance (int_hist_ticker)
            - Executive comps (int_exec_comps)
            - Stock prices (int_stock_prices)
            - Earnings transcripts (int_earnings_transcripts)
            - Financials (int_financials)
            - Websites (int_websites) or should this be URLs (int_urls)?
                - This is fact because we could, theoretically, scrape multiple websites for the same entity
                - This should have the URL, entity ID, scraped date, scraped data, Microlink response, and pdf file path
- The final step is the gold (or warehouse/mart) layer
    - We now conform the data into a conformed fact and dimension tables (i.e. no "fct_{table name}_{source system}" or "dim_{table name}_{source system}" but just "fct_{table name}" or "dim_{table name}")
    - We should add a table called "manifest" this lists out, for fact tables, the type of data we have for which durations
        - i.e. if we have daily data for last 2 years for AAPL from Polygon, but the user requests minute data for the past 2 years, we can say "sorry, we don't have that data" or "we can get that data for you but it will take a while"