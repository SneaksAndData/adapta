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
from hvac.api.auth_methods import Kubernetes

from adapta.security.clients._base import AuthenticationClient
from adapta.security.clients.hashicorp_vault.hashicorp_vault_client import (
    HashicorpVaultClient,
)


class HashicorpVaultKubernetesClient(HashicorpVaultClient):
    """
    Hashicorp vault Credentials provider for K8S.
    """

    @staticmethod
    def from_base_client(
        client: AuthenticationClient,
    ) -> Optional["HashicorpVaultKubernetesClient"]:
        """
         Safe casts AuthenticationClient to HashicorpVaultClient if type checks out.

        :param client: AuthenticationClient
        :return: HashicorpVaultKubernetesClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultKubernetesClient):
            return client

        return None

    def __init__(
        self,
        vault_address: str,
        deployment_cluster_name: str,
        kubernetes_token_path: str = "/var/run/secrets/kubernetes.io/serviceaccount/token",
    ):
        """
        Initialization logic for Kubernetes auth method
        :param vault_address: Address of hashicorp vault instance
        :param deployment_cluster_name: Name of kubernetes cluster where application is deployed.
        """
        super().__init__(vault_address)
        self._client = hvac.Client(url=self._vault_address)
        self.deployment_cluster_name = deployment_cluster_name
        self.token_path = kubernetes_token_path

    def get_credentials(self):
        with open(self.token_path, encoding="utf-8") as token_file:
            Kubernetes(self._client.adapter).login(
                role="application",
                jwt=token_file.read(),
                mount_point=f"kubernetes/{self.deployment_cluster_name}",
            )

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return self._client.token
