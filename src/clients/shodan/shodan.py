import requests #no sdk for python
import numpy as np
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

SHODAN_URL = 'https://entitydb.shodan.io/api/'

class Shodan_Client:
    def __init__(self, api_url: str = SHODAN_URL):
        self.api_url = api_url
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def health_check(self) -> bool:
        """Checks the health of the Shodan API.
        Returns:
            bool: True if the health check passed, False otherwise.
        """
        response = requests.get(self.api_url + 'health_check', headers=self.headers)
        
        if response.status_code != 200:
            logger.error(f"Health check failed: {response.status_code} - {response.text}")
            return False
            
        
        logger.info("Health check passed.")
        return True
        
    def get_all_entities(self, limit=100) -> requests.Response:
        """Fetches all entities from the Shodan API.
        Returns:
            requests.Response: The response object from the API call.
        """
        for i in range(0, limit, 100):
            params = {
                'limit': 100,
                'offset': i
            }
            response = requests.get(f"{self.api_url}/entities", headers=self.headers, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error fetching entities: {response.status_code} - {response.text}")
                raise Exception(f"Error fetching entities: {response.status_code} - {response.text}")
            
            logger.info(f"Fetched entities: {i}-{i+100}")
            yield response

    def get_entity_from_id(self, entity_id: str) -> requests.Response:
        """Fetches a specific entity from the Shodan API using its ID.
        Args:
            entity_id (str): The ID of the entity to fetch.
        Returns:
            requests.Response: The response object from the API call.
        """
        response = requests.get(f"{self.api_url}/entities/{entity_id}", headers=self.headers)
        
        if response.status_code != 200:
            logger.error(f"Error fetching entity: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching entity: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched entity: {entity_id}")
        return response
    
    def get_entity_from_symbol(self, symbol: str) -> requests.Response:
        """Fetches a specific entity from the Shodan API using its symbol.
        Args:
            symbol (str): The symbol of the entity to fetch.
        Returns:
            requests.Response: The response object from the API call.
        """
        response = requests.get(f"{self.api_url}/entities/symbol/{symbol}", headers=self.headers)
        
        if response.status_code != 200:
            logger.error(f"Error fetching entity: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching entity: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched entity: {symbol}")
        return response