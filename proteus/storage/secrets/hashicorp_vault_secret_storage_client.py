from typing import Union, Dict

import hvac

from proteus.security.clients import ProteusClient, HashicorpVaultClient
from proteus.storage.secrets import SecretStorageClient


class HashicorpSecretStorageClient(SecretStorageClient):
    """
    Hashicorp vault client
    """

    def __init__(self, *, base_client: ProteusClient, role: str = "default"):
        """
        Creates new instance
        :param base_client: HashicorpVaultClient backing this client.
        :param role: Name of role to log in with
        """
        super().__init__(base_client=base_client)
        self._base_client = HashicorpVaultClient.from_base_client(self._base_client)
        self._access_token = self._base_client.get_access_token()
        self.client = hvac.Client(self._base_client.vault_address, self._access_token)
        self.client.secrets.kv.v2.configure(max_versions=20, mount_point='secret')
        self._role = role

    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str]:
        secret = self.client.secrets.kv.v2.read_secret_version(path=secret_name)
        return secret["data"]["data"]

    def create_secret(self, storage_name: str, secret_name: str, secret_value: str, b64_encode=False) -> None:
        self.client.secrets.kv.v2.create_or_update_secret(path=secret_name, secret={"value": secret_value})

    def create_complex_secret(self, secret_name: str, secret_value: Dict[str, str]) -> None:
        self.client.secrets.kv.v2.create_or_update_secret(path=secret_name, secret=secret_value)
