This is a final project for the INFO 323 (Cloud Computing) course at Drexel University. 
The problem we are attempting to solve is how a lot of investors want to get up to date really quickly with 
company information so we want to create a platform that compiles information about the company like their performance, stock prices, a quick look at their website, and other information like executive compensation.

Current Plan:

1. **Data Collection**:
    - Create clients for different APIs to solve different problems
        - Microlink
            - Allows for the user to get a screenshot of their resume
            - We might enhance this to create a Duck Duck Go client to find the about section of the company
            - Allows the user to grab a PDF and text from the website
        - Shodan Entity DB
            - Has a list of a lot of public companies with their tickers and exchanges
            - We planned to use this to grab executive compensation but it hasn't returned anything for that yet
        - Financial Modeling Prep
            - Has a lot of information available like executive compensation, stock prices, and other information
            - Free tier is 250 calls a day but starter tier at $19 a month is 300 calls a minute
        - Polygon
            - Might use this for stock prices
        - Yfinance
            - Might use this for stock prices, earnings transcripts, etc.

2. **Data Storage**:
    - Use GCP Cloud Storage to store data

3. **Data Processing**:
    - Not sure yet
        - Current plan is BigQuery for processing but we might use Dataproc or Dataflow

4. **Data Visualization**:
    - Use Flask (with Streamlit or Dash) to create a web application that displays the data in a user-friendly way and allows for the user to interact with the data or request data that is not currently being held in GCP