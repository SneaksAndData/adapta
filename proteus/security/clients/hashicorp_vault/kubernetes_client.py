"""
 Hashicorp Vault implementation of Proteus Client.
"""
from typing import Optional

import hvac
from hvac.api.auth_methods import Kubernetes

from proteus.security.clients._base import ProteusClient
from proteus.security.clients.hashicorp_vault.hashicorp_vault_client import HashicorpVaultClient


class HashicorpVaultKubernetesClient(HashicorpVaultClient):
    """
     Hashicorp vault Credentials provider for K8S.
    """

    @staticmethod
    def from_base_client(client: ProteusClient) -> Optional['HashicorpVaultKubernetesClient']:
        """
         Safe casts ProteusClient to HashicorpVaultClient if type checks out.

        :param client: ProteusClient
        :return: HashicorpVaultKubernetesClient or None if type does not check out
        """
        if isinstance(client, HashicorpVaultKubernetesClient):
            return client

        return None

    def __init__(self,
                 vault_address: str,
                 deployment_cluster_name: str,
                 kubernetes_token_path: str = '/var/run/secrets/kubernetes.io/serviceaccount/token'):
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
        with open(self.token_path, encoding='utf-8') as token_file:
            Kubernetes(self._client.adapter).login(
                role='application',
                jwt=token_file.read(),
                mount_point=f'kubernetes/{self.deployment_cluster_name}'
            )

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return self._client.token
