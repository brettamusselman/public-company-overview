import requests #no sdk for python
import numpy as np
import pandas as pd
import logging
import io

#rate limiting/retry logic
from ratelimit import limits, sleep_and_retry
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import time

CALLS_PER_MINUTE = 300 #starter tier
ONE_MINUTE = 60

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MICROLINK_URL = 'https://api.microlink.io'

"""
Next steps:
- Add logic for parsing to different formats
- Add tenacity retry logic
- Potentially add schema validation/pydantic checks
"""

class Microlink_Client:
    def __init__(self, api_url: str = MICROLINK_URL):
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
    
    @staticmethod
    @sleep_and_retry
    @limits(calls=CALLS_PER_MINUTE, period=ONE_MINUTE)
    @retry(
        wait=wait_exponential(multiplier=1, min=2, max=10),
        stop=stop_after_attempt(5),
        retry=retry_if_exception_type((Exception,))
    )
    def _api_call(func, *args, **kwargs):
        """
        Safely calls an API function with rate limiting and retry.
        """
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Retrying after exception in API call: {e}")
            raise
        
    def get_url(self, url: str) -> requests.Response:
        """Fetches the URL using Microlink API.
        Args:
            url (str): The URL to fetch.
        Returns:
            requests.Response: The response object from the API call.
        """
        params = {
            'url': url
        }
        response = self._api_call(requests.get, self.api_url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Error fetching URL: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching URL: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched URL: {url}")
        return response
    
    def get_url_data(self, url: str) -> dict:
        """Fetches the URL data using Microlink API.
        Args:
            url (str): The URL to fetch.
        Returns:
            dict: The JSON response from the API call.
        """
        response = self.get_url(url)
        
        if response.status_code != 200:
            logger.error(f"Error fetching URL data: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching URL data: {response.status_code} - {response.text}")
        
        data = response.json()
        
        logger.info(f"Fetched URL data: {url}")
        return data
    
    def get_pdf(self, url: str) -> requests.Response:
        """Fetches the PDF from the URL using Microlink API.
        Args:
            url (str): The URL to fetch.
        Returns:
            requests.Response: The response object from the API call.
        """
        params = {
            'url': url,
            'pdf': True
        }
        response = self._api_call(requests.get, self.api_url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Error fetching PDF: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching PDF: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched PDF: {url}")
        return response
    
    def save_pdf_file(self, pdf_response: requests.Response) -> io.BytesIO:
        """Saves the PDF response to a file in memory.
        Args:
            pdf_response (requests.Response): The response object from the API call.
        Returns:
            io.BytesIO: The PDF file in memory.
        """
        if pdf_response.status_code != 200:
            logger.error(f"Error fetching PDF file: {pdf_response.status_code} - {pdf_response.text}")
            raise Exception(f"Error fetching PDF file: {pdf_response.status_code} - {pdf_response.text}")
        
        pdf_file = pdf_response.json()['data']['pdf']['url']

        #request the pdf file
        pdf_file_response = requests.get(pdf_file, timeout=120)

        if pdf_file_response.status_code != 200:
            logger.error(f"Error fetching PDF file: {pdf_file_response.status_code} - {pdf_file_response.text}")
            raise Exception(f"Error fetching PDF file: {pdf_file_response.status_code} - {pdf_file_response.text}")
        
        pdf_file = io.BytesIO(pdf_file_response.content)
        
        logger.info(f"PDF file saved in memory.")
        return pdf_file
    
    def get_pdf_file(self, url: str) -> io.BytesIO:
        """Fetches the PDF file from the URL using Microlink API.
        Args:
            url (str): The URL to fetch.
        Returns:
            io.BytesIO: The PDF file in memory.
        """
        pdf_response = self.get_pdf(url)
        pdf_file = self.save_pdf_file(pdf_response)
        
        logger.info(f"Fetched PDF file: {url}")
        return pdf_file