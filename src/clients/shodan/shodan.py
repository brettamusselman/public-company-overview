import requests #no sdk for python
import numpy as np
import pandas as pd
import io
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

SHODAN_URL = 'https://entitydb.shodan.io/api/'

"""
Next steps:
- Add pagination support logic
- Add tenacity retry logic
    - Use base _api_call method
- Change the object returned to be pandas dataframes
- Potentially add schema validation/pydantic checks use schemas.py file
"""

class Shodan_Client:
    def __init__(self, api_url: str = SHODAN_URL, api_key: str = None):
        self.api_url = api_url
        self.api_key = api_key
        self.key_str = f"?key={self.api_key}"
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def health_check(self) -> bool:
        """Checks the health of the Shodan API.
        Returns:
            bool: True if the health check passed, False otherwise.
        """
        response = requests.get(self.api_url + 'health_check' + self.key_str, headers=self.headers)
        
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
            response = requests.get(f"{self.api_url}entities{self.key_str}", headers=self.headers, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error fetching entities: {response.status_code} - {response.text}")
                raise Exception(f"Error fetching entities: {response.status_code} - {response.text}")
            
            logger.info(f"Fetched entities: {i}-{i+100}")
            yield response

    def all_entities_to_dataframe(self, limit=100) -> pd.DataFrame:
        """Fetches all entities from the Shodan API and converts them to a DataFrame.
        Args:
            limit (int): The maximum number of entities to fetch.
        Returns:
            pd.DataFrame: A DataFrame containing all entities.
        """
        all_entities = []
        for response in self.get_all_entities(limit):
            json_data = response.json()
            entities = json_data.get('entities', [])
            all_entities.extend(entities)

        #convert to dataframe
        df = pd.DataFrame(entities)
        logger.info(f"Converted {len(entities)} entities to DataFrame.")
        return df

    def get_entity_from_id(self, entity_id: str) -> requests.Response:
        """Fetches a specific entity from the Shodan API using its ID.
        Args:
            entity_id (str): The ID of the entity to fetch.
        Returns:
            requests.Response: The response object from the API call.
        """
        response = requests.get(f"{self.api_url}entities/{entity_id}{self.key_str}", headers=self.headers)
        
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
        response = requests.get(f"{self.api_url}entities/symbol/{symbol}{self.key_str}", headers=self.headers)
        
        if response.status_code != 200:
            logger.error(f"Error fetching entity: {response.status_code} - {response.text}")
            raise Exception(f"Error fetching entity: {response.status_code} - {response.text}")
        
        logger.info(f"Fetched entity: {symbol}")
        return response