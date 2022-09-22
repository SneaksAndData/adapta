from typing import Union

import hvac

from proteus.security.clients import ProteusClient, HashicorpVaultClient
from proteus.storage.secrets import SecretStorageClient


class HashicorpSecretStorageClient(SecretStorageClient):
    def __init__(self, *, base_client: ProteusClient, role="readwrite_secrets"):
        super().__init__(base_client=base_client)
        self._base_client = HashicorpVaultClient.from_base_client(self._base_client)
        self._access_token = self._base_client.get_access_token()
        self.client = hvac.Client()
        self._role = role

    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str]:
        response = self.client.auth.jwt.jwt_login(
            role=self._role,
            jwt=self._access_token
        )
        return response


    def create_secret(self, storage_name: str, secret_name: str, secret_value: str, b64_encode=False) -> None:
        pass
