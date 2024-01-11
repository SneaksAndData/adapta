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

from typing import Optional

import hvac

from adapta.security.clients._base import AuthenticationClient
from adapta.security.clients.hashicorp_vault.hashicorp_vault_client import (
    HashicorpVaultClient,
)


class HashicorpVaultTokenClient(HashicorpVaultClient):
    """
    Hashicorp vault Credentials provider for K8S.
    """

    @staticmethod
    def from_base_client(client: AuthenticationClient) -> Optional["HashicorpVaultTokenClient"]:
        """
         Safe casts AuthenticationClient to HashicorpVaultClient if type checks out.

        :param client: AuthenticationClient
        :return: HashicorpVaultKubernetesClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultTokenClient):
            return client

        return None

    def __init__(self, vault_address: str, access_token: str):
        """
        Initialization logic for Kubernetes auth method
        :param vault_address: Address of hashicorp vault instance
        """
        super().__init__(vault_address)
        self._client = hvac.Client(url=self._vault_address, token=access_token)

    def get_credentials(self):
        self.get_access_token()

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return self._client.token
