"""
 Azure Secret Storage Client (KeyVault).
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import base64
from typing import Union, Dict

from azure.keyvault.secrets import SecretClient

from adapta.storage.secrets import SecretStorageClient
from adapta.security.clients import AzureClient


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
        self,
        storage_name: str,
        secret_name: str,
        secret_value: Union[str, Dict[str, str]],
        b64_encode=False,
    ) -> None:
        if not isinstance(secret_value, str):
            raise ValueError(
                f"Only str secret type supported in AzureSecretStorageClient but was: {type(secret_value)}"
            )
        self._get_keyvault(storage_name).set_secret(
            secret_name,
            secret_value if not b64_encode else base64.b64encode(secret_value.encode("utf-8")),
        )

    def list_secrets(self, storage_name: str, name_prefix: str) -> None:
        raise NotImplementedError("Not supported  in AzureSecretStorageClient")
