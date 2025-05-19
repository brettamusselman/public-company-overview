import yfinance
import pandas as pd
import logging
import io

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class YF_Client:
    def __init__(self):
        self.client = yfinance
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def get_data(self, ticker: str, start: str, end: str) -> pd.DataFrame:
        """Fetches historical data for a given ticker.
        Args:
            ticker (str): The stock ticker symbol.
            start (str): The start date for the historical data (YYYY-MM-DD).
            end (str): The end date for the historical data (YYYY-MM-DD).
        Returns:
            pd.DataFrame: The historical data as a DataFrame.
        """
        try:
            data = self.client.download(ticker, start=start, end=end)
            logger.info(f"Fetched data for {ticker} from {start} to {end}")
            return data
        except Exception as e:
            logger.error(f"Exception occurred while fetching data: {str(e)}")
            raise

    def get_tickers(self) -> pd.DataFrame:
        """Fetches the list of all available tickers.
        Returns:
            pd.DataFrame: The list of tickers as a DataFrame.
        """
        try:
            tickers = self.client.Tickers()
            logger.info("Fetched all available tickers")
            return tickers
        except Exception as e:
            logger.error(f"Exception occurred while fetching tickers: {str(e)}")
            raise

    def get_ticker_info(self, ticker: str) -> pd.DataFrame:
        """Fetches information for a specific ticker.
        Args:
            ticker (str): The stock ticker symbol.
        Returns:
            pd.DataFrame: The ticker information as a DataFrame.
        """
        try:
            info = self.client.Ticker(ticker).info
            logger.info(f"Fetched information for {ticker}")
            return pd.DataFrame([info])
        except Exception as e:
            logger.error(f"Exception occurred while fetching ticker info: {str(e)}")
            raise

    def get_ticker_history(self, ticker: str, period: str = '1mo', interval: str = '1d') -> pd.DataFrame:
        """Fetches historical data for a given ticker.
        Args:
            ticker (str): The stock ticker symbol.
            period (str): The period for the historical data (e.g., '1mo', '1y').
            interval (str): The interval for the historical data (e.g., '1d', '1wk').
        Returns:
            pd.DataFrame: The historical data as a DataFrame.
        """
        try:
            history = self.client.Ticker(ticker).history(period=period, interval=interval)
            logger.info(f"Fetched historical data for {ticker} with period {period} and interval {interval}")
            return history
        except Exception as e:
            logger.error(f"Exception occurred while fetching ticker history: {str(e)}")
            raise

    def get_ticker_actions(self, ticker: str) -> pd.DataFrame:
        """Fetches corporate actions for a given ticker.
        Args:
            ticker (str): The stock ticker symbol.
        Returns:
            pd.DataFrame: The corporate actions as a DataFrame.
        """
        try:
            actions = self.client.Ticker(ticker).actions
            logger.info(f"Fetched corporate actions for {ticker}")
            return actions
        except Exception as e:
            logger.error(f"Exception occurred while fetching ticker actions: {str(e)}")
            raise

    def get_ticker_dividends(self, ticker: str) -> pd.DataFrame:
        """Fetches dividends for a given ticker.
        Args:
            ticker (str): The stock ticker symbol.
        Returns:
            pd.DataFrame: The dividends as a DataFrame.
        """
        try:
            dividends = self.client.Ticker(ticker).dividends
            logger.info(f"Fetched dividends for {ticker}")
            return dividends
        except Exception as e:
            logger.error(f"Exception occurred while fetching ticker dividends: {str(e)}")
            raise

    def get_ticker_splits(self, ticker: str) -> pd.DataFrame:
        """Fetches stock splits for a given ticker.
        Args:
            ticker (str): The stock ticker symbol.
        Returns:
            pd.DataFrame: The stock splits as a DataFrame.
        """
        try:
            splits = self.client.Ticker(ticker).splits
            logger.info(f"Fetched stock splits for {ticker}")
            return splits
        except Exception as e:
            logger.error(f"Exception occurred while fetching ticker splits: {str(e)}")
            raise

    