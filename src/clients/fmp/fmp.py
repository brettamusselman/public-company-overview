import requests
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

FMP_URL = 'https://financialmodelingprep.com/stable/'

class FMP_Client:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.key_str = f"&apikey={self.api_key}"
    
    #not sure if the get_data function will actually standardize as a base function
    def _get_data(self, endpoint: str, params: dict = None) -> pd.DataFrame:
        """Fetches data from the FMP API.
        Args:
            endpoint (str): The API endpoint to fetch data from.
            params (dict): Additional parameters for the API request.
        Returns:
            pd.DataFrame: The response data as a DataFrame.
        """
        try:
            if params is None:
                params = {}
            
            url = f"{FMP_URL}{endpoint}{self.key_str}"
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error fetching data: {response.status_code} - {response.text}")
                raise Exception(f"Error fetching data: {response.status_code} - {response.text}")
            
            logger.info(f"Fetched data from {url}")
            
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
        endpoint = f"income-statement?{symbol}"
        
        return self._get_data(endpoint)
    
    def get_key_executives(self, symbol: str) -> pd.DataFrame:
        """Fetches the key executives for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The key executives as a DataFrame.
        """
        endpoint = f"key-executives?{symbol}"
        
        return self._get_data(endpoint)
    
    def get_exec_comp(self, symbol: str) -> pd.DataFrame:
        """Fetches the executive compensation for a specific stock symbol.
        Args:
            symbol (str): The stock symbol to fetch data for.
        Returns:
            pd.DataFrame: The executive compensation as a DataFrame.
        """
        endpoint = f"governance-executive-compensation?{symbol}"
        
        return self._get_data(endpoint)
    
    