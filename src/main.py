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

#wrappers around GCP clients
from src.clients.storage.storage import Storage_Client
from src.clients.secrets.secrets import Secrets_Client
from src.clients.bq.bq import BQ_Client

#define functions
#right now these are quick placeholders that we have to enhance and "arbitrage" calls on the apis
def write_hist_prices(ticker: str, start: str, end: str, bucket_name: str, file_name: str):
    """Fetches historical prices for a given ticker and writes to GCS.
    Args:
        ticker (str): The stock ticker symbol.
        start (str): The start date for the historical data (YYYY-MM-DD).
        end (str): The end date for the historical data (YYYY-MM-DD).
        bucket_name (str): The GCS bucket name.
        file_name (str): The file name to write to GCS.
    """
    yf_client = YF_Client()
    storage_client = Storage_Client()
    
    # Fetch historical data
    data = yf_client.get_data(ticker, start, end)
    
    # Write to GCS
    storage_client.write_to_gcs(bucket_name, file_name, data.to_csv(index=False))

def write_microlink_pdf(url: str, bucket_name: str, file_name: str):
    """Fetches a PDF from a URL using Microlink and writes to GCS.
    Args:
        url (str): The URL to fetch the PDF from.
        bucket_name (str): The GCS bucket name.
        file_name (str): The file name to write to GCS.
    """
    microlink_client = Microlink_Client()
    storage_client = Storage_Client()
    
    # Fetch PDF
    pdf_response = microlink_client.get_pdf(url)
    
    # Write to GCS
    storage_client.write_to_gcs(bucket_name, file_name, pdf_response.content)

