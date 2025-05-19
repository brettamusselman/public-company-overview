from polygon import RESTClient
import pandas as pd
from polygon import WebSocketClient
from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Polygon_Wrapper:
    def __init__(self, api_key: str):
        self.rest_client = RESTClient(api_key)
        self.ws_client = WebSocketClient(api_key)
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    def get_historical_data(self, 
                            ticker: str, 
                            start: str, 
                            end: str, 
                            timespan="day", 
                            multiplier=1, 
                            adjusted="true", 
                            sort="asc", 
                            limit=50000) -> pd.DataFrame:
        """Fetches historical data for a given ticker.
        Args:
            ticker (str): The stock ticker symbol.
            start (str): The start date for the historical data (YYYY-MM-DD).
            end (str): The end date for the historical data (YYYY-MM-DD).
            timespan (str): The timespan to use (day, minute, hour, etc.).
            multiplier (int): The multiplier to use with the timespan.
            adjusted (str): Whether to use adjusted prices ("true" or "false").
            sort (str): The sort order of the results ("asc" or "desc").
            limit (int): Maximum number of results to return.
        Returns:
            pd.DataFrame: The historical data as a DataFrame.
        """
        try:
            aggs = []
            for a in self.rest_client.list_aggs(
                ticker,
                multiplier,
                timespan,
                start,
                end,
                adjusted=adjusted,
                sort=sort,
                limit=limit,
            ):
                aggs.append(a)
            
            logger.info(f"Fetched historical data for {ticker} from {start} to {end}")
            return pd.DataFrame(aggs)
        except Exception as e:
            logger.error(f"Exception occurred while fetching historical data: {str(e)}")
            raise

        