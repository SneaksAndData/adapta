"""
 Hashicorp Vault Secret storage client
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

from typing import Union, Dict, Iterable

import hvac

from adapta.security.clients import AuthenticationClient, HashicorpVaultClient
from adapta.storage.secrets import SecretStorageClient


class HashicorpSecretStorageClient(SecretStorageClient):
    """
    Hashicorp vault client.
    """

    def __init__(self, *, base_client: AuthenticationClient, role: str = "default"):
        """
        Creates new instance
        :param base_client: HashicorpVaultClient backing this client.
        :param role: Name of role to log in with
        """
        super().__init__(base_client=base_client)
        self._base_client = HashicorpVaultClient.from_base_client(self._base_client)
        self._access_token = self._base_client.get_access_token()
        self.client = hvac.Client(self._base_client.vault_address, self._access_token)
        self._role = role

    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str, Dict[str, str]]:
        secret = self.client.secrets.kv.v2.read_secret_version(path=secret_name)
        return secret["data"]["data"]

    def create_secret(
        self,
        storage_name: str,
        secret_name: str,
        secret_value: Union[str, Dict[str, str]],
        b64_encode=False,
    ) -> None:
        if not isinstance(secret_value, Dict):
            raise ValueError(
                f"Only Dict secret type supported in HashicorpSecretStorageClient but was: {type(secret_value)}"
            )
        self.client.secrets.kv.v2.create_or_update_secret(path=secret_name, secret=secret_value)

    def list_secrets(self, storage_name: str, name_prefix: str) -> Iterable[str]:
        stack = [name_prefix]
        while stack:
            name = stack.pop(0)
            keys = self.client.secrets.kv.v2.list_secrets(path=name, mount_point=storage_name)["data"]["keys"]
            for key in keys:
                if self._is_key(key):
                    yield self._combine_path(name, key)
                else:
                    stack.append(self._combine_path(name, key))

    @staticmethod
    def _combine_path(*args):
        return "/".join([arg.strip("/") for arg in args])

    @staticmethod
    def _is_key(name):
        return not name.endswith("/")
