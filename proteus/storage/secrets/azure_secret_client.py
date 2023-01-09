"""
 Azure Secret Storage Client (KeyVault).
"""
import base64
from typing import Union, Dict

from azure.keyvault.secrets import SecretClient

from proteus.storage.secrets import SecretStorageClient
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

    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str, Dict[str, str]]:
        return self._get_keyvault(storage_name).get_secret(secret_name).value

    def create_secret(
        self, storage_name: str, secret_name: str, secret_value: Union[str, Dict[str, str]], b64_encode=False
    ) -> None:
        if not isinstance(secret_value, str):
            raise ValueError(
                f"Only str secret type supported in AzureSecretStorageClient but was: {type(secret_value)}"
            )
        self._get_keyvault(storage_name).set_secret(
            secret_name, secret_value if not b64_encode else base64.b64encode(secret_value.encode("utf-8"))
        )

    def list_secrets(self, storage_name: str, name_prefix: str) -> None:
        raise NotImplementedError("Not supported  in AzureSecretStorageClient")
