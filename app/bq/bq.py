import os
import logging
import requests
import pandas as pd
import polars as pl
from io import BytesIO
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import google.auth

from bq.queries import available_tickers, available_sources, stock_data_query_template

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_PROJECT = "public-company-overview"
DEFAULT_DATASET = "public-company-overview.pco_dataset"

def is_running_on_gcp():
    try:
        r = requests.get("http://metadata.google.internal", headers={"Metadata-Flavor": "Google"}, timeout=0.1)
        return r.status_code == 200
    except requests.exceptions.RequestException:
        return False

from google.auth import default as google_auth_default

class BQ_Client:
    def __init__(self, project_id: str = DEFAULT_PROJECT, dataset_id: str = DEFAULT_DATASET):
        self.project_id = project_id
        self.dataset_id = dataset_id

        try:
            if is_running_on_gcp():
                self.client = bigquery.Client(project=project_id)
            else:
                # Automatically loads ADC from ~/.config/gcloud/ if available
                creds, _ = google_auth_default()
                self.client = bigquery.Client(credentials=creds, project=project_id)
        except Exception as e:
            logger.error("Failed to initialize BigQuery client: %s", e)
            raise RuntimeError(f"BigQuery client initialization failed: {e}")

    def _get_table_ref(self, table_id: str):
        return self.client.dataset(self.dataset_id).table(table_id)

    def table_exists(self, table_id: str) -> bool:
        try:
            self.client.get_table(self._get_table_ref(table_id))
            return True
        except NotFound:
            return False

    def load_from_gcs(self, table_id: str, gcs_uri: str, file_format: str = 'CSV', write_disposition: str = 'WRITE_APPEND', schema: list = None):
        """
        Load a file from GCS into BigQuery.

        Parameters:
        - table_id: str — Name of the target table.
        - gcs_uri: str — URI of the file (e.g., gs://bucket/path/file.csv).
        - file_format: str — One of 'CSV', 'JSON', 'PARQUET'.
        - write_disposition: str — 'WRITE_APPEND', 'WRITE_TRUNCATE', or 'WRITE_EMPTY'.
        - schema: list — Optional BigQuery schema if table doesn't exist and file format needs it.
        """
        try:
            job_config = bigquery.LoadJobConfig(
                source_format=getattr(bigquery.SourceFormat, file_format),
                autodetect=schema is None,
                write_disposition=write_disposition,
            )

            if schema:
                job_config.schema = schema

            table_ref = self._get_table_ref(table_id)
            load_job = self.client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
            load_job.result()  # Wait for the job to complete

            logger.info(f"Loaded data from {gcs_uri} into {self.project_id}:{self.dataset_id}.{table_id}")
        except Exception as e:
            logger.error(f"Error loading data from {gcs_uri} into {self.project_id}:{self.dataset_id}.{table_id}: {str(e)}")
            raise

    def query(self, sql: str):
        """
        Run a SQL query and return the result as a list of dictionaries.

        Parameters:
        - sql: str — SQL query string

        Returns:
        - List[Dict]: Query results
        """
        try:
            query_job = self.client.query(sql)
            results = query_job.result()
            logger.info(f"Executed query: {sql}")
            return results
        except Exception as e:
            logger.error(f"Error executing query: {sql} - {str(e)}")
            raise

    def get_dataframe(self, sql: str, use_polars: bool = False):
        """
        Run a SQL query and return the result as a DataFrame.
        """
        try:
            results = self.query(sql)
            if use_polars:
                df = pd.DataFrame([dict(row) for row in results])
                return pl.DataFrame(df)
            else:
                return results.to_dataframe()
        except Exception as e:
            logger.error(f"Error fetching dataframe from query: {sql} - {str(e)}")
            raise

    def get_available_tickers(self) -> list:
        df = self.get_dataframe(available_tickers)
        return df['Ticker'].tolist()

    def get_available_sources(self) -> list:
        df = self.get_dataframe(available_sources)
        return df['DataSource'].dropna().tolist()

    def get_single_stock(self, ticker: str, start_date, end_date, granularity: str = 'D', use_polars: bool = False):
        """
        Fetch stock data for a single ticker between two dates and given granularity.

        Parameters:
        - ticker: str — The stock ticker
        - start_date, end_date: datetime.date — Date range
        - granularity: str — One of 'H', 'D', 'M', 'Y'
        - use_polars: bool — Return a polars DataFrame if True

        Returns:
        - DataFrame (pandas or polars)
        """
        start_str = start_date.strftime('%Y-%m-%d')
        end_str = end_date.strftime('%Y-%m-%d')

        base_unit = {
            'H': 'hour',
            'D': 'day',
            'M': 'month',
            'Y': 'year'
        }.get(granularity.upper(), 'day')

        query = stock_data_query_template.format(
            ticker=ticker,
            start_date=start_str,
            end_date=end_str,
            base_unit=base_unit
        )
;s
        return self.get_dataframe(query, use_polars=use_polars)
