from typing import Optional, Dict

import hvac
from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath


class VaultClient(ProteusClient):

    def get_credentials(self):
        pass

    def get_access_token(self, scope: Optional[str] = None) -> str:
        pass

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        pass

    def connect_account(self):
        pass