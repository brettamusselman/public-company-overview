from google.cloud import storage
from io import BytesIO
from typing import Union
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

DEFAULT_BUCKET = "dbt-testing"

class GCS_Client_Wrapper:
    def __init__(self, bucket_name: str = DEFAULT_BUCKET):
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)

    def upload_object(self, destination_path: str, data: Union[str, bytes, BytesIO], content_type: str = 'application/octet-stream'):
        """Uploads in-memory data to GCS.
        Args:
            destination_path (str): The path in GCS where the object will be stored.
            data (Union[str, bytes, BytesIO]): The data to upload. Can be a string, bytes, or BytesIO object.
                - Want to extend this to support dataframes with an automatic conversion functionality.
            content_type (str): The content type of the uploaded file.
        """
        blob = self.bucket.blob(destination_path)

        if isinstance(data, str):
            blob.upload_from_string(data, content_type=content_type)
        elif isinstance(data, bytes):
            blob.upload_from_string(data, content_type=content_type)
        elif isinstance(data, BytesIO):
            data.seek(0)
            blob.upload_from_file(data, content_type=content_type)
        else:
            logger.error(f"Unsupported data type for upload: {type(data)}")
            raise TypeError("Unsupported data type for upload.")

        logger.info(f"Uploaded to gs://{self.bucket.name}/{destination_path}")

    def read_object(self, source_path: str) -> bytes:
        """Reads the object from GCS and returns it as bytes.
        Args:
            source_path (str): The path in GCS from where the object will be read.
        Returns:
            bytes: The content of the object.
        """
        blob = self.bucket.blob(source_path)
        if not blob.exists():
            logger.error(f"Object not found: gs://{self.bucket.name}/{source_path}")
            raise FileNotFoundError(f"Object not found: gs://{self.bucket.name}/{source_path}")
        logger.info(f"Reading from gs://{self.bucket.name}/{source_path}")
        return blob.download_as_bytes()
