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
from clients.yfinance.yfinance import YF_Client
from clients.fmp.fmp import FMP_Client
from clients.microlink.microlink import Microlink_Client
#from clients.shodan.shodan import Shodan_Client #might not use this
from clients.polygon.polygon import Polygon_Wrapper

#wrappers around GCP clients
from clients.storage.storage import GCS_Client_Wrapper
from clients.secrets.secrets import Secret_Manager
#from clients.bq.bq import BQ_Client #might not use this

#define functions
def _write_base(client_func, file_path_func, *args, content_type='text/csv', **kwargs):
    """Generic function to call an API method, get data, and write it to GCS.
    Args:
        client_func (Callable): Function to call for fetching data.
        file_path_func (Callable): Function to generate the output file path.
        *args: Positional args for the client_func.
        content_type (str): Content type for upload.
        **kwargs: Keyword args for client_func.
    """
    storage_client = GCS_Client_Wrapper()

    logger.info(f"Calling {client_func.__name__} with args: {args}, kwargs: {kwargs}")
    data = client_func(*args, **kwargs)

    # If it's a DataFrame, convert to CSV
    if isinstance(data, pd.DataFrame):
        output = data.to_csv(index=False)
    elif isinstance(data, (dict, list)):
        output = json.dumps(data)
        content_type = 'application/json'
    elif isinstance(data, io.BytesIO):
        output = data
    else:
        raise ValueError("Unsupported data type for upload")

    file_path = file_path_func(*args, **kwargs)
    logger.info(f"Writing file to: {file_path}")
    storage_client.upload_object(file_path, output, content_type=content_type)

def _generate_file_path(prefix: str, *args) -> str:
    """Generates a file path based on the prefix and arguments."""
    current_time = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    sanitized_args = [re.sub(r'\W+', '_', str(arg)) for arg in args]
    arg_str = '_'.join(sanitized_args)
    return f"{prefix}/{current_time}_{arg_str}.csv"

# Define functions for each API call using the base methods
def write_hist_prices_yf(ticker: str, start: str, end: str):
    """Fetches historical prices for a given ticker and writes to GCS.
    Args:
        ticker (str): The stock ticker symbol.
        start (str): The start date for the historical data (YYYY-MM-DD).
        end (str): The end date for the historical data (YYYY-MM-DD).
    """
    file_path = lambda *args, **kwargs: _generate_file_path("yfinance/hist_ticker/daily", ticker)
    yf_client = YF_Client()
    _write_base(yf_client.get_data, file_path, ticker, start, end)

def write_hist_tickers_yf(tickers: list, period: str, interval: str):
    """Fetches historical ticker data for multiple tickers and writes to GCS.
    Args:
        tickers (list): List of stock ticker symbols.
        period (str): The period for the historical data (e.g., '1mo').
        interval (str): The interval for the historical data (e.g., '1d').
    """
    file_path = lambda *args, **kwargs: _generate_file_path(f"yfinance/hist_tickers/interval/{interval}", tickers)
    yf_client = YF_Client()
    _write_base(yf_client.get_tickers_history, file_path, tickers, period, interval)

def write_hist_ticker_yf(ticker: str, period: str, interval: str):
    """Fetches historical ticker data for a ticker and writes to GCS.
    Args:
        tickers (str): ticker symbol
        period (str): The period for the historical data (e.g., '1mo').
        interval (str): The interval for the historical data (e.g., '1d').
    """
    file_path = lambda *args, **kwargs: _generate_file_path(f"yfinance/hist_ticker/interval/{interval}", ticker)
    yf_client = YF_Client()
    _write_base(yf_client.get_ticker_history, file_path, ticker, period, interval)

def write_hist_ticker_polygon(ticker: str, start: str, end: str, timespan="day", multiplier=1, adjusted="true"):
    """Fetches historical ticker data from Polygon and writes to GCS.
    Args:
        ticker (str): The stock ticker symbol.
        start (str): The start date for the historical data (YYYY-MM-DD).
        end (str): The end date for the historical data (YYYY-MM-DD).
        timespan (str): The timespan to use (day, minute, hour, etc.).
        multiplier (int): The multiplier to use with the timespan.
        adjusted (str): Whether to use adjusted prices ("true" or "false").
    """
    secret_manager = Secret_Manager()
    polygon_api_key = secret_manager.get_secret("pco-polygon")
    polygon_client = Polygon_Wrapper(polygon_api_key)
    file_name = lambda *args, **kwargs: _generate_file_path(f"polygon/hist_ticker/interval/{multiplier}{timespan}", ticker)
    _write_base(polygon_client.get_historical_data, file_name, ticker, start, end, timespan=timespan, multiplier=multiplier, adjusted=adjusted)

def write_microlink_pdf(url: str):
    """Fetches a PDF from a URL using Microlink and writes to GCS.
    Args:
        url (str): The URL to fetch the PDF from.
    """
    microlink_client = Microlink_Client()

    file_path = lambda *args, **kwargs: _generate_file_path("microlink/pdf", url)
    _write_base(microlink_client.get_pdf_file, file_path, url, content_type='application/pdf')

def write_microlink_text(url: str):
    """Fetches text from a URL using Microlink and writes to GCS.
    Args:
        url (str): The URL to fetch the text from.
    """
    microlink_client = Microlink_Client()
    file_path = lambda *args, **kwargs: _generate_file_path("microlink/text", url)
    _write_base(microlink_client.get_url_data, file_path, url, content_type='application/json')

def write_tickers_polygon():
    secret_manager = Secret_Manager()
    polygon_api_key = secret_manager.get_secret("pco-polygon")
    polygon_client = Polygon_Wrapper(polygon_api_key)
    file_path = lambda *args, **kwargs: _generate_file_path("polygon/tickers")
    _write_base(polygon_client.get_tickers, file_path)

def write_exchanges_polygon():
    secret_manager = Secret_Manager()
    polygon_api_key = secret_manager.get_secret("pco-polygon")
    polygon_client = Polygon_Wrapper(polygon_api_key)
    file_path = lambda *args, **kwargs: _generate_file_path("polygon/exchanges")
    _write_base(polygon_client.get_exchanges, file_path)

def write_hist_ticker_fmp(ticker: str, start: str, end: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/hist_ticker/daily", ticker)
    _write_base(fmp_client.get_ticker_history, file_path, ticker, start, end)

def write_hist_ticker_interval_fmp(ticker: str, interval: str, from_date: str, to_date: str, nonadjusted: bool = False):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path(f"fmp/hist_ticker/interval/{interval}", ticker)
    _write_base(fmp_client.get_ticker_history_interval, file_path, ticker, interval, from_date, to_date, nonadjusted=nonadjusted)

def write_company_profile_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/company_profile", ticker)
    _write_base(fmp_client.get_company_profile, file_path, ticker)

def write_company_notes_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/company_notes", ticker)
    _write_base(fmp_client.get_company_notes, file_path, ticker)

def write_stock_peers_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/stock_peers", ticker)
    _write_base(fmp_client.get_stock_peers, file_path, ticker)

def write_employee_count_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/employee_count", ticker)
    _write_base(fmp_client.get_employee_count, file_path, ticker)

def write_income_statement_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/income_statement", ticker)
    _write_base(fmp_client.get_income_statement, file_path, ticker)

def write_balance_sheet_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/balance_sheet", ticker)
    _write_base(fmp_client.get_balance_sheet, file_path, ticker)

def write_cash_flow_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/cash_flow", ticker)
    _write_base(fmp_client.get_cash_flow, file_path, ticker)

def write_stock_news_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/stock_news", ticker)
    _write_base(fmp_client.get_stock_news, file_path, ticker)

def write_key_executives_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/key_executives", ticker)
    _write_base(fmp_client.get_key_executives, file_path, ticker)

def write_exec_comp_fmp(ticker: str):
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/exec_comp", ticker)
    _write_base(fmp_client.get_exec_comp, file_path, ticker)

def write_tickers_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/tickers")
    _write_base(fmp_client.get_tickers, file_path)

def write_tickers_w_financials_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/tickers_w_financials")
    _write_base(fmp_client.get_tickers_with_financials, file_path)

def write_ciks_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/ciks")
    _write_base(fmp_client.get_ciks, file_path)

def write_exchanges_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/exchanges")
    _write_base(fmp_client.get_exchanges, file_path)

def write_countries_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/countries")
    _write_base(fmp_client.get_countries, file_path)

def write_industries_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/industries")
    _write_base(fmp_client.get_industries, file_path)

def write_sectors_fmp():
    secret_manager = Secret_Manager()
    fmp_key = secret_manager.get_secret("pco-fmp")
    fmp_client = FMP_Client(fmp_key)
    file_path = lambda *args, **kwargs: _generate_file_path("fmp/sectors")
    _write_base(fmp_client.get_sectors, file_path)

def write_tickers():
    """
    This function should be the main dimension table for "tickers" representing different public companies.
    """
    write_tickers_fmp()
    write_tickers_w_financials_fmp()
    write_tickers_polygon()

def write_exchanges():
    """
    This function should be the main dimension table for "exchanges" representing different stock exchanges.
    """
    write_exchanges_fmp()
    write_exchanges_polygon()

def write_dimensions():
    """
    This function should be the main dimension table for "dimensions" representing different dimensions.
    """
    write_tickers()
    write_exchanges()
    write_countries_fmp()
    write_industries_fmp()
    write_sectors_fmp()

def write_facts(list_of_tickers: list):
    """
    This function should be the main fact table for "facts" representing different facts.
    Args:
        list_of_tickers (list): List of tickers to pull data for.
    """
    #get yesterday's date
    yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")

    #get 363 days before yesterday
    start_date = pd.Timestamp.now() - pd.Timedelta(days=363)
    start_date = start_date.strftime("%Y-%m-%d")

    for ticker in list_of_tickers:
        try:
            write_hist_ticker_yf(ticker, "2y", "1d")
        except Exception as e:
            logging.warning(f"Failed to fetch historical data for {ticker} from YF: {e}")
        try:
            write_hist_ticker_polygon(ticker, "2y", "1d", timespan="day", multiplier=1, adjusted="true")
        except Exception as e:
            logging.warning(f"Failed to fetch historical data for {ticker} from Polygon: {e}")
        try:
            write_hist_ticker_fmp(ticker, start_date, yesterday)
        except Exception as e:
            logging.warning(f"Failed to fetch historical data for {ticker} from FMP: {e}")
        try:
            write_balance_sheet_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch balance sheet data for {ticker} from FMP: {e}")
        try:  
            write_income_statement_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch income statement data for {ticker} from FMP: {e}")
        try:
            write_cash_flow_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch cash flow data for {ticker} from FMP: {e}")
        try:
            write_stock_peers_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch stock peers data for {ticker} from FMP: {e}")
        try:
            write_stock_news_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch stock news data for {ticker} from FMP: {e}")
        try:
            write_company_profile_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch company profile data for {ticker} from FMP: {e}")
        try:
            write_company_notes_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch company notes data for {ticker} from FMP: {e}")
        try:
            write_employee_count_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch employee count data for {ticker} from FMP: {e}")
        try:
            write_key_executives_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch key executives data for {ticker} from FMP: {e}")
        try:
            write_exec_comp_fmp(ticker)
        except Exception as e:
            logging.warning(f"Failed to fetch executive compensation data for {ticker} from FMP: {e}")

def standard_workflow(ticker: str):
    """
    This function should represent a standard workflow where a ticker is entered and a bunh of data is pulled.
    """
    #grab daily ticker history for the last 2 years
    write_hist_ticker_yf(ticker, "2y", "1d")
    yesterday = pd.Timestamp.now() - pd.Timedelta(days=1)
    yesterday = yesterday.strftime("%Y-%m-%d")
    start_date = pd.Timestamp.now() - pd.Timedelta(days=1800) #polygon start
    start_date = start_date.strftime("%Y-%m-%d")
    write_hist_ticker_polygon(ticker, start_date, yesterday, timespan="day", multiplier=1, adjusted="true")
    start_date = pd.Timestamp.now() - pd.Timedelta(days=1800) #change this since Starter tier gets up to 5 years of historical data
    start_date = start_date.strftime("%Y-%m-%d")
    write_hist_ticker_fmp(ticker, start_date, yesterday)

    #get key executives, exec comp, company profile, company notes etc.
    write_key_executives_fmp(ticker)
    write_exec_comp_fmp(ticker)
    write_company_profile_fmp(ticker)
    write_company_notes_fmp(ticker)
    write_employee_count_fmp(ticker)
    write_stock_peers_fmp(ticker)
    write_stock_news_fmp(ticker)
    write_income_statement_fmp(ticker)
    write_balance_sheet_fmp(ticker)
    write_cash_flow_fmp(ticker)
    
    #add this later but get url and get pdf and text from microlink
    yf_client = YF_Client()
    ticker_info = yf_client.get_ticker_info(ticker)
    url = ticker_info.get('website')
    write_microlink_pdf(url)
    write_microlink_text(url)

def daily_hist_ticker():
    """
    This function is for daily historical ticker data, pointing it to yf first, then fmp, then polygon.
    It will limit the amount of data that can be retrieved to a certain amount.
    """
    pass

def daily_update():
    """
    This function should represent a daily update workflow where data is pulled on a daily basis.
    It should be called by a cron job or similar with a focus on a list of top stocks and dimensions.
    """
    write_dimensions()

    #add list of tickers to pull and their respective fact table functions below
    sp500 = pd.read_excel('sp500.xlsx')
    list_of_tickers = sp500['Ticker'].tolist()
    list_of_tickers = list_of_tickers[:50]
    list_of_tickers = [ticker.split('-')[0] for ticker in list_of_tickers]
    write_facts(list_of_tickers)

#we could separate this into a different module and probably do the same with the write functions
def cli_args() -> argparse.Namespace:
    """
    This function should handle command line arguments.
    It should parse the arguments and call the appropriate functions.
    """
    parser = argparse.ArgumentParser(description="Fetch data from various APIs and write to GCS.")
    
    # Add arguments
    parser.add_argument("--yf-hist-prices", help="Fetch historical prices for a given ticker", action="store_true")
    parser.add_argument("--yf-hist-ticker", help="Fetch historical ticker data", action="store_true")
    parser.add_argument("--polygon-hist-ticker", help="Fetch historical ticker data from Polygon", action="store_true")
    parser.add_argument("--microlink-pdf", help="Fetch PDF from a URL using Microlink", action="store_true")
    parser.add_argument("--microlink-text", help="Fetch text from a URL using Microlink", action="store_true")
    parser.add_argument("--fmp-tickers", help="Fetch tickers from FMP", action="store_true")
    parser.add_argument("--fmp-exchanges", help="Fetch exchanges from FMP", action="store_true")
    parser.add_argument("--polygon-tickers", help="Fetch tickers from Polygon", action="store_true")
    parser.add_argument("--polygon-exchanges", help="Fetch exchanges from Polygon", action="store_true")
    parser.add_argument("--fmp-ciks", help="Fetch CIKs from FMP", action="store_true")
    parser.add_argument("--fmp-hist-ticker", help="Fetch historical ticker data from FMP", action="store_true")
    parser.add_argument("--fmp-hist-ticker-interval", help="Fetch historical ticker data with interval from FMP", action="store_true")
    parser.add_argument("--fmp-company-profile", help="Fetch company profile from FMP", action="store_true")
    parser.add_argument("--fmp-company-notes", help="Fetch company notes from FMP", action="store_true")
    parser.add_argument("--fmp-stock-peers", help="Fetch stock peers from FMP", action="store_true")
    parser.add_argument("--fmp-employee-count", help="Fetch employee count from FMP", action="store_true")
    parser.add_argument("--fmp-income-statement", help="Fetch income statement from FMP", action="store_true")
    parser.add_argument("--fmp-balance-sheet", help="Fetch balance sheet from FMP", action="store_true")
    parser.add_argument("--fmp-cash-flow", help="Fetch cash flow from FMP", action="store_true")
    parser.add_argument("--fmp-stock-news", help="Fetch stock news from FMP", action="store_true")
    parser.add_argument("--fmp-key-executives", help="Fetch key executives from FMP", action="store_true")
    parser.add_argument("--fmp-exec-comp", help="Fetch executive compensation from FMP", action="store_true")
    parser.add_argument("--fmp-countries", help="Fetch countries from FMP", action="store_true")
    parser.add_argument("--fmp-industries", help="Fetch industries from FMP", action="store_true")
    parser.add_argument("--fmp-sectors", help="Fetch sectors from FMP", action="store_true")
    parser.add_argument("--fmp-tickers-w-financials", help="Fetch tickers with financials from FMP", action="store_true")
    parser.add_argument("--write-dimensions", help="Write all dimensions", action="store_true")
    parser.add_argument("--daily-update", help="Run daily update workflow", action="store_true")
    parser.add_argument("--standard-workflow", help="Run standard workflow", action="store_true")
    
    # Add more arguments as needed
    parser.add_argument("--ticker", help="Ticker symbol")
    parser.add_argument("--tickers", help="List of ticker symbols (comma-separated)")
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--period", help="Period for historical data (e.g., '1mo')")
    parser.add_argument("--interval", help="Interval for historical data (e.g., '1d')")
    parser.add_argument("--url", help="URL to fetch data from")
    parser.add_argument("--timespan", help="Timespan for historical data (e.g., 'day')")
    parser.add_argument("--multiplier", help="Multiplier for historical data", type=int, default=1)
    parser.add_argument("--adjusted", help="Whether to use adjusted prices (true/false)", default="true")
    parser.add_argument("--content-type", help="Content type for upload", default="text/csv")
    parser.add_argument("--prefix", help="Prefix for file path")

    return parser.parse_args()

def main():
    args = cli_args()

    if args.yf_hist_prices:
        if not (args.ticker and args.start and args.end):
            logger.error("Missing required arguments for --yf-hist-prices: --ticker, --start, --end")
            return
        write_hist_prices_yf(args.ticker, args.start, args.end)

    if args.yf_hist_ticker:
        if not (args.ticker and args.period and args.interval):
            logger.error("Missing required arguments for --yf-hist-ticker: --ticker, --period, --interval")
            return
        write_hist_ticker_yf(args.ticker, args.period, args.interval)

    if args.polygon_hist_ticker:
        if not (args.ticker and args.start and args.end):
            logger.error("Missing required arguments for --polygon-hist-ticker: --ticker, --start, --end")
            return
        write_hist_ticker_polygon(
            ticker=args.ticker,
            start=args.start,
            end=args.end,
            timespan=args.timespan or "day",
            multiplier=args.multiplier,
            adjusted=args.adjusted
        )

    if args.microlink_pdf:
        if not args.url:
            logger.error("Missing required argument for --microlink-pdf: --url")
            return
        write_microlink_pdf(args.url)

    if args.microlink_text:
        if not args.url:
            logger.error("Missing required argument for --microlink-text: --url")
            return
        write_microlink_text(args.url)

    if args.fmp_hist_ticker:
        if not (args.ticker and args.start and args.end):
            logger.error("Missing required arguments for --fmp-hist-ticker: --ticker, --start, --end")
            return
        write_hist_ticker_fmp(args.ticker, args.start, args.end)

    if args.fmp_hist_ticker_interval:
        if not (args.ticker and args.interval and args.from_date and args.to_date):
            logger.error("Missing required arguments for --fmp-hist-ticker-interval: --ticker, --interval, --from_date, --to_date")
            return
        write_hist_ticker_interval_fmp(args.ticker, args.interval, args.from_date, args.to_date)

    if args.fmp_tickers:
        write_tickers_fmp()

    if args.fmp_tickers_w_financials:
        write_tickers_w_financials_fmp()

    if args.fmp_exchanges:
        write_exchanges_fmp()

    if args.fmp_countries:
        write_countries_fmp()

    if args.fmp_industries:
        write_industries_fmp()

    if args.fmp_sectors:
        write_sectors_fmp()

    if args.fmp_ciks:
        write_ciks_fmp()

    if args.fmp_company_profile:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-company-profile: --ticker")
            return
        write_company_profile_fmp(args.ticker)

    if args.fmp_company_notes:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-company-notes: --ticker")
            return
        write_company_notes_fmp(args.ticker)

    if args.fmp_stock_peers:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-stock-peers: --ticker")
            return
        write_stock_peers_fmp(args.ticker)

    if args.fmp_employee_count:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-employee-count: --ticker")
            return
        write_employee_count_fmp(args.ticker)

    if args.fmp_income_statement:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-income-statement: --ticker")
            return
        write_income_statement_fmp(args.ticker)

    if args.fmp_balance_sheet:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-balance-sheet: --ticker")
            return
        write_balance_sheet_fmp(args.ticker)

    if args.fmp_cash_flow:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-cash-flow: --ticker")
            return
        write_cash_flow_fmp(args.ticker)

    if args.fmp_stock_news:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-stock-news: --ticker")
            return
        write_stock_news_fmp(args.ticker)

    if args.fmp_key_executives:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-key-executives: --ticker")
            return
        write_key_executives_fmp(args.ticker)

    if args.fmp_exec_comp:
        if not args.ticker:
            logger.error("Missing required argument for --fmp-exec-comp: --ticker")
            return
        write_exec_comp_fmp(args.ticker)

    if args.polygon_tickers:
        write_tickers_polygon()

    if args.polygon_exchanges:
        write_exchanges_polygon()

    if args.write_dimensions:
        write_dimensions()

    if args.daily_update:
        daily_update()

    if args.standard_workflow:
        if not args.ticker:
            logger.error("Missing required argument for --standard-workflow: --ticker")
            return
        standard_workflow(args.ticker)

"""
Next Steps:
- Finish api_server.py to call this script
- Deploy to Cloud Run with Docker container
- Enhance logic for each of the functions to handle errors and edge cases
- Alot of the code is getting repetitive, might be good to make a base function to build off of
"""

if __name__ == "__main__":
    main()