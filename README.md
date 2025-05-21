This is a final project for the INFO 323 (Cloud Computing) course at Drexel University. 
The problem we are attempting to solve is how a lot of investors want to get up to date really quickly with 
company information so we want to create a platform that compiles information about the company like their performance, stock prices, a quick look at their website, and other information like executive compensation.

Current Plan:

1. **Data Collection**:
    - Create clients for different APIs to solve different problems
        - Microlink
            - Allows for the user to get a screenshot of the company website
            - We might enhance this with a search engine to search for about section (Bing API, DuckDuck Go) or we could use a web crawler like Scrapy + BeautifulSoup using the given URLs from many of the entity tables from the source APIs
            - Allows the user to grab a PDF and text from the website
        - Financial Modeling Prep
            - This is the main library for financial data for this project
            - Has a lot of information available like executive compensation, stock prices, and other information
            - Paid for starter tier
        - Polygon
            - Might use this for stock prices
        - Yfinance
            - Might use this for stock prices, earnings transcripts, etc.
            - This one is very useful but we might want to rate limit this
        - Shodan
            - Planned on using this but doesn't seem as useful anymore
        - Storage
            - Wrapper around GCP Cloud Storage to store the data
        - Secrets
            - Wrapper around GCP Secret Manager to store the API keys
        - BigQuery
            - Wrapper around GCP BigQuery to read the data into tables
    - Wrap with a main.py that accepts command line args
    - Add a FastAPI to handle the correct calls
    - Host on Cloud Run in Docker container

2. **Data Storage**:
    - Use GCP Cloud Storage to store data

3. **Data Processing**:
    - Not sure yet (most likely DBT + BigQuery)
        - Current plan is BigQuery for processing abstracted through DBT
        - See database_design/design.md for more information

4. **Data Visualization**:
    - Use Flask (with Streamlit or Dash) to create a web application that displays the data in a user-friendly way and allows for the user to interact with the data or request data that is not currently being held in GCP
        - Flask app with Dash in iframes or separate pages

**Project Structure:**
- src/ directory holds clients directory with wrappers around the APIs as well as main.py and api_server.py
- utils/ directory holds utility functions for generating initial data
- app/ directory holds the Flask + Dash app
- database_design/ directory holds the database design and schema
- dbt_project/ directory holds the DBT project

**Possible Improvements:**
- Switch write functions to use DLT
- Add a web crawler to scrape the website for more information or find the write information
- Add an AI model to analyze the data and provide insights and talk to the user about the information
- Wrap some AutoML functionality to allow the user to forecast or find anomalies in the data