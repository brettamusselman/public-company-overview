import requests #no sdk for python
import numpy as np
import pandas as pd
import logging
import io

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
        response = requests.get(self.api_url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Error fetching URL: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching URL: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched URL: {url}")
        return response
    
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
        response = requests.get(self.api_url, headers=self.headers, params=params)
        
        if response.status_code != 200:
            logger.error(f"Error fetching PDF: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching PDF: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched PDF: {url}")
        return response
    
    def get_pdf_file(self, pdf_response: requests.Response) -> io.BytesIO.:
        """Saves the PDF response to a file in memory.
        Args:
            pdf_response (requests.Response): The response object from the API call.
        Returns:
            io.BytesIO: The PDF file in memory.
        """
        if pdf_response.status_code != 200:
            logger.error(f"Error fetching PDF file: {pdf_response.status_code} - {pdf_response.text}")
            raise Exception(f"Error fetching PDF file: {pdf_response.status_code} - {pdf_response.text}")
        
        pdf_file = pdf_response.content['data']['pdf']['url']

        #request the pdf file
        pdf_file_response = requests.get(pdf_file)

        if pdf_file_response.status_code != 200:
            logger.error(f"Error fetching PDF file: {pdf_file_response.status_code} - {pdf_file_response.text}")
            raise Exception(f"Error fetching PDF file: {pdf_file_response.status_code} - {pdf_file_response.text}")
        
        pdf_file = io.BytesIO(pdf_file_response.content)
        
        logger.info(f"PDF file saved in memory.")
        return pdf_file