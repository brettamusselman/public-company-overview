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
- Add more functions (there are tons of endpoints but we can get away with some of the more basic stuff)
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
    def _get_data(self, endpoint: str, params: dict = None, version='v3', stable_end=False) -> pd.DataFrame:
        """Fetches data from the FMP API.
        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (dict): Additional parameters for the API request.
            version (str): The API version to use.
            stable_end (bool): Whether to fetch from stable.
        Returns:
            pd.DataFrame: The response data as a DataFrame.
        """
        try:
            if params is None:
                params = {}
            
            # Add API key to query params
            params["apikey"] = self.api_key

            url = f"{FMP_URL}{version}/{endpoint}"
            if stable_end: #for some reason, this api url doesn't want the api/ part
                url = f"{FMP_URL.replace("api/", "")}{version}/{endpoint}"
            response = self._api_call(requests.get, url, headers=self.headers, params=params)
            
            if response.status_code != 200:
                logger.error(url.replace(self.key_str, "api=<hidden>"))
                logger.error(f"Error fetching data: {response.status_code} - {response.text}")
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")
            
            logger.info(f"Fetched data from {url}")
            
            # Convert the response to a DataFrame
            data = pd.read_json(io.StringIO(response.text))
            
            return data
        except Exception as e:
            logger.error(f"Exception occurred while fetching data: {str(e)}")
            raise

    def get_ticker_history(self, symbol: str, from_date: str = None, to_date: str = None) -> pd.DataFrame:
        """Fetches comprehensive historical price and volume data for a specific stock symbol.
        
        Args:
            symbol (str): The stock symbol to fetch data for.
            from_date (str, optional): Start date in YYYY-MM-DD format.
            to_date (str, optional): End date in YYYY-MM-DD format.
            
        Returns:
            pd.DataFrame: Historical price and volume data including open, high, low, close,
                         volume, price changes, and volume-weighted average price (VWAP).
        """
        endpoint = "historical-price-eod/full"
        params = {"symbol": symbol}
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
            
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)
    
    def get_ticker_history_interval(self, symbol: str, interval: str = '4h', from_date: str = None, to_date: str = None,
                                    nonadjusted: bool = False) -> pd.DataFrame:
        """Fetches historical price and volume data for a specific stock symbol with a specified interval.
        
        Args:
            symbol (str): The stock symbol to fetch data for.
            interval (str): The interval for the historical data (e.g., '1d', '1h', '4h').
            from_date (str, optional): Start date in YYYY-MM-DD format.
            to_date (str, optional): End date in YYYY-MM-DD format.
            nonadjusted (bool): Whether to fetch non-adjusted data.
            
        Returns:
            pd.DataFrame: Historical price and volume data including open, high, low, close,
                         volume, price changes, and volume-weighted average price (VWAP).
        """
        allowed_intervals = ['1min', '5min', '15min', '30min', '1h', '4h']
        if interval not in allowed_intervals:
            raise ValueError(f"Invalid interval. Allowed intervals are: {allowed_intervals}")
        
        endpoint = f"historical-chart/{interval}"
        params = {"symbol": symbol}
        
        if from_date:
            params["from"] = from_date
        if to_date:
            params["to"] = to_date
        if nonadjusted:
            params["nonadjusted"] = nonadjusted
            
        return self._get_data(endpoint, params=params)
    
    def get_company_profile(self, symbol: str) -> pd.DataFrame:
        """Fetches the company profile for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The company profile as a DataFrame.
        """
        endpoint = f"profile/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_company_notes(self, symbol: str) -> pd.DataFrame:
        """Fetches the company notes for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The company notes as a DataFrame.
        """
        endpoint = f"company-notes"
        params = {
            "symbol": symbol
        }
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)
    
    def get_stock_peers(self, symbol: str) -> pd.DataFrame:
        """Fetches the stock peers for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The stock peers as a DataFrame.
        """
        endpoint = f"stock-peers"
        params = {
            "symbol": symbol
        }
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)
    
    def get_employee_count(self, symbol: str) -> pd.DataFrame:
        """Fetches the employee count for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The employee count as a DataFrame.
        """
        endpoint = f"employee-count"
        params = {
            "symbol": symbol
        }
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)
    
    def get_income_statement(self, symbol: str) -> pd.DataFrame:
        """Fetches the income statement for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The income statement as a DataFrame.
        """
        endpoint = f"income-statement/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_balance_sheet(self, symbol: str) -> pd.DataFrame:
        """Fetches the balance sheet for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The balance sheet as a DataFrame.
        """
        endpoint = f"balance-sheet-statement/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_cash_flow(self, symbol: str) -> pd.DataFrame:
        """Fetches the cash flow statement for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The cash flow statement as a DataFrame.
        """
        endpoint = f"cash-flow-statement/{symbol}"
        
        return self._get_data(endpoint)
    
    def get_stock_news(self, symbol: str) -> pd.DataFrame:
        """Fetches the stock news for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The stock news as a DataFrame.
        """
        endpoint = f"news/stock"
        params = {
            "symbol": symbol
        }
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)
    
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
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)
    
    def get_stock_earning_transcripts(self, symbol: str,
                                       year: str,
                                       quarter: str) -> pd.DataFrame:
        """
        NOTE: NOT AVAILABLE UNTIL PREMIUM TIER AND WE HAVE STARTER
        Fetches the earnings transcripts for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
            year (str): The year of the earnings transcript.
            quarter (str): The quarter of the earnings transcript.
        Returns:
            pd.DataFrame: The earnings transcripts as a DataFrame.
        """
        endpoint = f"earning-call-transcript"
        params = {"symbol": symbol, "year": year, "quarter": quarter}
        return self._get_data(endpoint, params=params, version='stable', stable_end=True)

    def company_name_search(self, query: str):
        """Searches for a company by name.
        Args:
            query (str): The company name to search for.
        Returns:
            pd.DataFrame: The search results as a DataFrame.
        """
        endpoint = f"search-name?query={query}"
        
        return self._get_data(endpoint)

    def get_tickers(self) -> pd.DataFrame:
        """Fetches the list of all tickers.
        Returns:
            pd.DataFrame: The list of tickers as a DataFrame.
        """
        endpoint = "stock-list"
        return self._get_data(endpoint, version='stable', stable_end=True)

    def get_tickers_with_financials(self) -> pd.DataFrame:
        """Fetches the list of tickers with financials.
        Returns:
            pd.DataFrame: The list of tickers with financials as a DataFrame.
        """
        endpoint = "financial-statement-symbol-list"
        return self._get_data(endpoint, version='stable', stable_end=True)

    def get_ciks(self) -> pd.DataFrame:
        """Fetches the list of CIKs.
        Returns:
            pd.DataFrame: The list of CIKs as a DataFrame.
        """
        endpoint = "cik-list"
        return self._get_data(endpoint, version='stable', stable_end=True)

    def get_exchanges(self) -> pd.DataFrame:
        """Fetches the list of exchanges.
        Returns:
            pd.DataFrame: The list of exchanges as a DataFrame.
        """
        endpoint = "available-exchanges"
        return self._get_data(endpoint, version='stable', stable_end=True)

    def get_sectors(self) -> pd.DataFrame:
        """Fetches the list of sectors.
        Returns:
            pd.DataFrame: The list of sectors as a DataFrame.
        """
        endpoint = "available-sectors"
        return self._get_data(endpoint, version='stable', stable_end=True)

    def get_industries(self) -> pd.DataFrame:
        """Fetches the list of industries.
        Returns:
            pd.DataFrame: The list of industries as a DataFrame.
        """
        endpoint = "available-industries"
        return self._get_data(endpoint, version='stable', stable_end=True)

    def get_countries(self) -> pd.DataFrame:
        """Fetches the list of countries.
        Returns:
            pd.DataFrame: The list of countries as a DataFrame.
        """
        endpoint = "available-countries"
        return self._get_data(endpoint, version='stable', stable_end=True)