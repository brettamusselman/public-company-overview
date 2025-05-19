from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
#polars is faster than pandas and can handle more data than pandas so might be good for reading from bq
import polars as pl #we might want to use polars for performance reasons when in App and using memory
import logging
from io import BytesIO

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_PROJECT = "public-company-overview"
#should dataset just be pco_dataset without the prefix?
DEFAULT_DATASET = "public-company-overview.pco_dataset"

class BQ_Client:
    def __init__(self, project_id: str = DEFAULT_PROJECT, dataset_id: str = DEFAULT_DATASET):
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
        self.dataset_id = dataset_id

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
        except Exception as e:
            logger.error(f"Error executing query: {sql} - {str(e)}")
            raise