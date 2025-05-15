import requests #no sdk for python
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

MICROLINK_URL = 'https://api.microlink.io'

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
        
        return response
    
    