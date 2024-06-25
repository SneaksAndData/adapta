"""
 Hashicorp Vault implementation of AuthenticationClient.
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

from abc import abstractmethod
from typing import Optional, Dict

from pyarrow.fs import FileSystem

from adapta.security.clients._base import AuthenticationClient
from adapta.storage.models.base import DataPath


class HashicorpVaultClient(AuthenticationClient):
    """
    Hashicorp vault Credentials provider.
    """

    @staticmethod
    def from_base_client(
        client: AuthenticationClient,
    ) -> Optional["HashicorpVaultClient"]:
        """
         Safe casts AuthenticationClient to HashicorpVaultClient if type checks out.

        :param client: AuthenticationClient
        :return: HashicorpVaultClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultClient):
            return client

        return None

    def __init__(self, vault_address: str):
        """
        Common initialization logic for hashicorp vault clients
        :param vault_address: Address of hashicorp vault instance
        """
        self._vault_address = vault_address

    @property
    def vault_address(self):
        """Returns address of Hashicorp Vault server"""
        return self._vault_address

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Not supported  in HashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    def connect_account(self):
        """
         Not supported  in HashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        """
         Not supported  in HashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    def initialize_session(self, session_callable=None) -> "LocalClient":
        """
         Not supported  in HashicorpVaultClient
        :return:
        """
        raise ValueError("Not supported  in HashicorpVaultClient")

    @abstractmethod
    def get_credentials(self):
        pass

    @abstractmethod
    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass
