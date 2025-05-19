from polygon import StocksClient
import pandas as pd
#from polygon import WebSocketClient
#from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Polygon_Wrapper:
    def __init__(self, api_key: str):
        self.rest_client = StocksClient(api_key)
        #self.ws_client = WebSocketClient(api_key)
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
                            limit=50000,
                            full_range=True,
                            run_parallel=True,
                            max_concurrent_workers=10) -> pd.DataFrame:
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
            full_range (bool): Whether to get the entire date range and merge responses.
            run_parallel (bool): Whether to use parallel processing for fetching data.
            max_concurrent_workers (int): Maximum number of concurrent workers.
        Returns:
            pd.DataFrame: The historical data as a DataFrame.
        """
        try:
            # Convert adjusted string to boolean
            adjusted_bool = adjusted.lower() == "true" if isinstance(adjusted, str) else adjusted
            
            # Get aggregate bars
            aggs = self.rest_client.get_aggregate_bars(
                symbol=ticker,
                from_date=start,
                to_date=end,
                adjusted=adjusted_bool,
                sort=sort,
                limit=limit,
                multiplier=multiplier,
                timespan=timespan,
                full_range=full_range,
                run_parallel=run_parallel,
                max_concurrent_workers=max_concurrent_workers
            )
            
            logger.info(f"Fetched historical data for {ticker} from {start} to {end}")
            return pd.DataFrame(aggs)
        except Exception as e:
            logger.error(f"Exception occurred while fetching historical data: {str(e)}")
            raise

