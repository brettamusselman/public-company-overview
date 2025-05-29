import requests
import logging
import google.auth
import google.auth.transport.requests
import google.oauth2.id_token

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DEFAULT_AUDIENCE = "https://pco-545002904663.us-east1.run.app"

class API:
    """
    API client for interacting with a Cloud Run-based service.
    """

    def __init__(self, audience: str = DEFAULT_AUDIENCE):
        """
        Initializes the API client with a Cloud Run service URL.

        Args:
            audience (str): The base URL of the Cloud Run service.
        """
        self.audience = audience
        self.auth_request = google.auth.transport.requests.Request()

    def get_id_token(self) -> str:
        """
        Fetches an identity token for the Cloud Run service.

        Returns:
            str: A valid Google-signed identity token.
        """
        try:
            return google.oauth2.id_token.fetch_id_token(self.auth_request, self.audience)
        except Exception as e:
            logger.error(f"Failed to fetch ID token: {e}")
            raise RuntimeError(f"ID token fetch error: {e}")

    def invoke_standard_workflow(self, ticker: str = "AAPL") -> dict:
        """
        Invokes the /standard-workflow endpoint of the Cloud Run service with a ticker.

        Args:
            ticker (str): The stock ticker symbol to include in the request payload.

        Returns:
            dict: Parsed JSON response from the Cloud Run service.

        Raises:
            RuntimeError: If the request fails or the response is invalid.
        """
        endpoint = "/standard-workflow"
        url = f"{self.audience}{endpoint}"
        payload = {
            "args": ["--standard-workflow", "--ticker", ticker]
        }

        try:
            logger.info(f"Fetching ID token for audience: {self.audience}")
            id_token = self.get_id_token()

            headers = {
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json",
            }

            logger.info(f"Sending POST request to {url} with payload: {payload}")
            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()

            logger.info(f"Response received with status {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as req_err:
            logger.error(f"HTTP request to {url} failed: {req_err}")
            raise RuntimeError(f"Failed to call Cloud Run endpoint: {req_err}")
        except ValueError as json_err:
            logger.error(f"Invalid JSON response from {url}: {json_err}")
            raise RuntimeError(f"Invalid JSON response: {json_err}")
        except Exception as e:
            logger.error(f"Unexpected error invoking workflow: {e}")
            raise RuntimeError(f"Unexpected error: {e}")

    def invoke_daily_update(self) -> dict:
        endpoint = "/daily-update"
        try:
            logger.info(f"Fetching ID token for audience: {self.audience}")
            id_token = self.get_id_token()

            headers = {
                "Authorization": f"Bearer {id_token}",
                "Content-Type": "application/json",
            }

            logger.info(f"Sending POST request to {url}.")
            response = requests.post(url, headers=headers)
            response.raise_for_status()

            logger.info(f"Response received with status {response.status_code}")
            return response.json()
        except requests.exceptions.RequestException as req_err:
            logger.error(f"HTTP request to {url} failed: {req_err}")
            raise RuntimeError(f"Failed to call Cloud Run endpoint: {req_err}")
        except ValueError as json_err:
            logger.error(f"Invalid JSON response from {url}: {json_err}")
            raise RuntimeError(f"Invalid JSON response: {json_err}")
        except Exception as e:
            logger.error(f"Unexpected error invoking workflow: {e}")
            raise RuntimeError(f"Unexpected error: {e}")