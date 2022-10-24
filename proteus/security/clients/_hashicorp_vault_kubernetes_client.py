"""
 Hashicorp Vault implementation of Proteus Client.
"""
from typing import Optional

import hvac
from hvac.api.auth_methods import Kubernetes

from proteus.security.clients import HashicorpVaultClient


class HashicorpVaultKubernetesClient(HashicorpVaultClient):
    """
     Hashicorp vault Credentials provider.
    """

    def __init__(self, vault_address, deployment_cluster_name):
        super().__init__(vault_address)
        self._client = hvac.Client(url=self._vault_address)
        self._deployment_cluster_name = deployment_cluster_name

    def get_credentials(self):
        with open('/var/run/secrets/kubernetes.io/serviceaccount/token', encoding='utf-8') as token_file:
            Kubernetes(self._client.adapter).login(
                role='application',
                jwt=token_file.read(),
                mount_point=f'kubernetes/{self._deployment_cluster_name}'
            )

    def get_access_token(self, scope: Optional[str] = None) -> str:
        return self._client.token
