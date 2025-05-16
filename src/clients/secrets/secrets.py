from google.cloud import secretmanager
from typing import Optional
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

class Secret_Manager:
    def __init__(self, project_id: str):
        self.project_id = project_id
        #Found this here: https://codelabs.developers.google.com/codelabs/secret-manager-python#5
        #Might be outdated
        self.client = secretmanager.SecretManagerServiceClient()

    def _secret_path(self, secret_id: str, version: str = "latest") -> str:
        return f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"

    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """Fetch the secret value from GCP Secrets Manager.
        Args:
            secret_id (str): The ID of the secret to fetch.
            version (str): The version of the secret to fetch. Defaults to "latest".
        Returns:
            str: The secret value.
        """
        name = self._secret_path(secret_id, version)
        try:
            response = self.client.access_secret_version(request={"name": name})
        except Exception as e:
            logger.error(f"Error accessing secret version: {e}")
            raise
        logger.info(f"Accessed secret version: {name}")
        return response.payload.data.decode("UTF-8")

    def list_secrets(self):
        """List all secrets in the project."""
        parent = f"projects/{self.project_id}"
        return [secret.name for secret in self.client.list_secrets(request={"parent": parent})]
