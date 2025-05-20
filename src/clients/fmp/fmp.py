import requests
import pandas as pd
import io
import logging
import json

#rate limiting/retry logic
from ratelimit import limits, sleep_and_retry
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import time

CALLS_PER_MINUTE = 300 #starter tier
ONE_MINUTE = 60

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

FMP_URL = 'https://financialmodelingprep.com/api/'

"""
Next Steps:
- Add functions for M&A, Market Cap, Headcount, Company Notes, Stock Peers, Analyst ratings, News, and Earning Transcripts
"""

class FMP_Client:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.key_str = f"apikey={self.api_key}"

    @staticmethod
    @sleep_and_retry
    @limits(calls=CALLS_PER_MINUTE, period=ONE_MINUTE)
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((Exception,))
    )
    def _api_call(func, *args, **kwargs):
        """
        Safely calls an API function with rate limiting and retry.
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Retrying after exception in API call: {e}")
            raise
    
    #not sure if the get_data function will actually standardize as a base function
    def _get_data(self, endpoint: str, params: dict = None, version='v3', exec_comp=False) -> pd.DataFrame:
        """Fetches data from the FMP API.
        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (dict): Additional parameters for the API request.
            version (str): The API version to use.
            exec_comp (bool): Whether to fetch executive compensation data.
        Returns:
            pd.DataFrame: The response data as a DataFrame.
        """
        try:
            if params is None:
                params = {}
            
            # Add API key to query params
            params["apikey"] = self.api_key

            url = f"{FMP_URL}{version}/{endpoint}"
            if exec_comp: #for some reason, this api url doesn't want the api/ part
                url = f"{FMP_URL.replace("api/", "")}{version}/{endpoint}"
            response = self._api_call(requests.get, url, headers=self.headers, params=params)
            
            if response.status_code != 200:
                logger.error(url.replace(self.key_str, "api=<hidden>"))
                logger.error(f"Error fetching data: {response.status_code} - {response.text}")
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")
            
            logger.info(f"Fetched data from {url}")
            print(f"response.content: {response.content}")
            
            # Convert the response to a DataFrame
            data = pd.read_json(io.StringIO(response.text))
            
            return data
        except Exception as e:
            logger.error(f"Exception occurred while fetching data: {str(e)}")
            raise
    
    def get_company_profile(self, symbol: str) -> pd.DataFrame:
        """Fetches the company profile for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The company profile as a DataFrame.
        """
        endpoint = f"profile/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """Fetches the income statement for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The income statement as a DataFrame.
        """
        endpoint = f"income-statement/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_key_executives(self, symbol: str) -> pd.DataFrame:
        """Fetches the key executives for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The key executives as a DataFrame.
        """
        endpoint = f"key-executives/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_exec_comp(self, symbol: str) -> pd.DataFrame:
        """Fetches the executive compensation for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The executive compensation as a DataFrame.
        """
        endpoint = "governance-executive-compensation"
        params = {"symbol": symbol}
        return self._get_data(endpoint, params=params, version='stable', exec_comp=True)

    
