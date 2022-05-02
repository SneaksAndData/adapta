"""
 Azure Secret Storage Client (KeyVault).
"""
import base64
from typing import Union

from azure.keyvault.secrets import SecretClient

from proteus.storage.secrets.base import SecretStorageClient
from proteus.security.clients import AzureClient


class AzureSecretStorageClient(SecretStorageClient):
    """
     Azure KeyVault Client.
    """
    def __init__(self, *, base_client: AzureClient):
        """
         Creates a new instance of AzureSecretStorageClient.

        :param base_client: AzureClient backing this client.
        """
        super().__init__(base_client=base_client)
        self._base_client = AzureClient.from_base_client(self._base_client)

    def _get_keyvault(self, keyvault: str) -> SecretClient:
        kv_uri = f"https://{keyvault}.vault.azure.net"
        return SecretClient(kv_uri, self._base_client.get_credentials())

    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str]:
        kv = self._get_keyvault(storage_name)
        return kv.get_secret(secret_name).value

    def create_secret(self, storage_name: str, secret_name: str, secret_value: str, b64_encode=False) -> None:
        kv = self._get_keyvault(storage_name)
        kv.set_secret(secret_name, secret_value if not b64_encode else base64.b64encode(secret_value.encode('utf-8')))
