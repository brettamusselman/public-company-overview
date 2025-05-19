#This is the file where we handle the main logic of the program.
#From here, we orchestrate the clients using command line arguments.
#What this means is we can set this up as a cloud function or something similar and have it be called by code in the app.

#import the necessary libraries
import requests
import json
import pandas as pd
import logging
import io
import argparse
import re

# Set up logging
# This sets it up so the way we have logging in the clients will route to this logger
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

#custom clients around APIs/SDKs
from src.clients.yfinance.yfinance import YF_Client
from src.clients.fmp.fmp import FMP_Client
from src.clients.microlink.microlink import Microlink_Client
from src.clients.shodan.shodan import Shodan_Client
from src.clients.polygon.polygon import Polygon_Wrapper

#wrappers around GCP clients
from src.clients.storage.storage import GCS_Client_Wrapper
from src.clients.secrets.secrets import Secret_Manager
from src.clients.bq.bq import BQ_Client

#define functions
#right now these are quick placeholders that we have to enhance and "arbitrage" calls on the apis
def write_hist_prices_yf(ticker: str, start: str, end: str, bucket_name: str, file_name: str):
    """Fetches historical prices for a given ticker and writes to GCS.
    Args:
        ticker (str): The stock ticker symbol.
        start (str): The start date for the historical data (YYYY-MM-DD).
        end (str): The end date for the historical data (YYYY-MM-DD).
        bucket_name (str): The GCS bucket name.
        file_name (str): The file name to write to GCS.
    """
    yf_client = YF_Client()
    storage_client = GCS_Client_Wrapper()
    
    # Fetch historical data
    data = yf_client.get_data(ticker, start, end)
    
    # Write to GCS
    storage_client.upload_object(bucket_name, file_name, data.to_csv(index=False))

def write_hist_ticker_yf(ticker: str, period: str, interval: str):
    """Fetches historical ticker data and writes to GCS.
    Args:
        ticker (str): The stock ticker symbol.
        period (str): The period for the historical data (e.g., '1mo').
        interval (str): The interval for the historical data (e.g., '1d').
    """
    yf_client = YF_Client()
    storage_client = GCS_Client_Wrapper()

    #should add error handling + arg handling (i.e. max period and interval options)
    
    # Fetch historical data
    data = yf_client.get_ticker_history(ticker, period, interval)

    # Set file name
    current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"{ticker}_{period}_{interval}_{current_time}.csv"
    
    # Write to GCS
    storage_client.upload_object(file_name, data.to_csv(index=False), content_type='text/csv')

def write_microlink_pdf(url: str):
    """Fetches a PDF from a URL using Microlink and writes to GCS.
    Args:
        url (str): The URL to fetch the PDF from.
    """
    microlink_client = Microlink_Client()
    storage_client = GCS_Client_Wrapper()
    
    # Fetch PDF
    pdf_file = microlink_client.get_pdf_file(url)

    # Set filename
    current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    sanitized_url = re.sub(r'\W+', '_', url)  # Replace non-word chars with underscores
    file_name = f"{sanitized_url}_{current_time}.pdf"
    
    # Write to GCS
    storage_client.upload_object(file_name, pdf_file, content_type='application/pdf')

def write_entities():
    """
    This function should be the main dimension table for "entities" representing different public companies.
    Not sure which source system we should use for this yet. (shodan, polygon, yfinance, fmp?)
    """
    pass