from polygon import StocksClient, ReferenceClient
import pandas as pd
#from polygon import WebSocketClient
#from polygon.websocket.models import WebSocketMessage, Feed, Market
from typing import List
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

"""
Notes:
- Don't use the official Polygon.io documentation (for some reason), use pip polygon docs and maybe github

Next Steps:
- 
"""

class Polygon_Wrapper:
    def __init__(self, api_key: str):
        self.rest_client = StocksClient(api_key)
        self.ref_client = ReferenceClient(api_key)
        #self.ws_client = WebSocketClient(api_key)
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def get_tickers(self, limit=1000, exchange="") -> pd.DataFrame:
        """
        Fetches all available ticker symbols from Polygon.io.
        
        Handles pagination automatically to retrieve the complete list of tickers.

        args:
            limit (int): The maximum number of tickers to fetch. Default is 1000.
            exchange (str): The exchange to filter tickers by. Default is empty string (no filter).
        
        Returns:
            pd.DataFrame: DataFrame containing ticker information including symbol, name, market, etc.
        
        Raises:
            Exception: If there is an error fetching tickers from the API.
        """
        try:
            logger.info("Starting to fetch tickers from Polygon.io")
            all_tickers = []
            
            # Get first page of results
            response = self.ref_client.get_tickers(limit=limit, exchange="")
            if not response or "results" not in response:
                logger.warning("No results found in the initial tickers response")
                return pd.DataFrame()
                
            all_tickers.extend(response.get("results", []))
            logger.info(f"Fetched initial page with {len(response.get('results', []))} tickers")
            
            # Handle pagination
            page_count = 1
            while "next_url" in response:
                try:
                    page_count += 1
                    logger.debug(f"Fetching page {page_count} of tickers")
                    response = self.ref_client.get_next_page(response)
                    if response and "results" in response:
                        all_tickers.extend(response.get("results", []))
                        logger.debug(f"Added {len(response.get('results', []))} tickers from page {page_count}")
                    else:
                        logger.warning(f"No results found in page {page_count}")
                        break
                except Exception as e:
                    logger.error(f"Error fetching page {page_count}: {str(e)}")
                    break
            
            # Create dataframe from all ticker results
            df = pd.DataFrame(all_tickers)
            logger.info(f"Successfully retrieved {len(df)} tickers in total")
            return df
        
        except Exception as e:
            logger.error(f"Exception occurred while fetching tickers: {str(e)}")
            raise

    def get_tickers_on_date(self, date: str, limit=1000, exchange="") -> pd.DataFrame:
        """Fetches all available ticker symbols on a specific date.
        
        Handles pagination automatically to retrieve the complete list of tickers.
        
        Args:
            date (str): The date to filter tickers by (YYYY-MM-DD).
            limit (int): The maximum number of tickers to fetch. Default is 1000.
            exchange (str): The exchange to filter tickers by. Default is empty string (no filter).
        
        Returns:
            pd.DataFrame: DataFrame containing ticker information including symbol, name, market, etc.
        
        Raises:
            Exception: If there is an error fetching tickers from the API.
        """
        try:
            logger.info(f"Starting to fetch tickers on date {date}")
            all_tickers = []
            
            # Get first page of results
            response = self.ref_client.get_tickers(date=date, limit=limit, exchange=exchange)
            if not response or "results" not in response:
                logger.warning("No results found in the initial tickers on date response")
                return pd.DataFrame()
                
            all_tickers.extend(response.get("results", []))
            logger.info(f"Fetched initial page with {len(response.get('results', []))} tickers on date {date}")
            
            # Handle pagination
            page_count = 1
            while "next_url" in response:
                try:
                    page_count += 1
                    logger.debug(f"Fetching page {page_count} of tickers on date {date}")
                    response = self.ref_client.get_next_page(response)
                    if response and "results" in response:
                        all_tickers.extend(response.get("results", []))
                        logger.debug(f"Added {len(response.get('results', []))} tickers from page {page_count}")
                    else:
                        logger.warning(f"No results found in page {page_count}")
                        break
                except Exception as e:
                    logger.error(f"Error fetching page {page_count}: {str(e)}")
                    break
            
            # Create dataframe from all ticker results
            df = pd.DataFrame(all_tickers)
            logger.info(f"Successfully retrieved {len(df)} tickers on date {date} in total")
            return df
        except Exception as e:
            logger.error(f"Exception occurred while fetching tickers on date: {str(e)}")
            raise
    
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

