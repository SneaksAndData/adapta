from typing import Optional, Dict

import hvac
from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath


class VaultClient(ProteusClient):

    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        pass

    def connect_account(self):
        pass

    def __init__(self):
        self._client = hvac.Client()

    def get_credentials(self):
        with open('/var/run/secrets/kubernetes.io/serviceaccount/token') as f:
            jwt = f.read()
            self._client.auth_kubernetes("application", jwt)
