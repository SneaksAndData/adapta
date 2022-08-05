from ctypes import Union

import hvac

from proteus.security.clients import ProteusClient
from proteus.storage.secrets import SecretStorageClient


class HashicorpSecretStorageClient(SecretStorageClient):
    def __init__(self, *, base_client: ProteusClient):
        super().__init__(base_client=base_client)

    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str]:
        pass

    def create_secret(self, storage_name: str, secret_name: str, secret_value: str, b64_encode=False) -> None:
        pass
