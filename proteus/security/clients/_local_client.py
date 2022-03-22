from typing import Optional, Dict

from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath


class LocalClient(ProteusClient):
    def get_credentials(self):
        """
         Not supported in local client.
        :return:
        """
        pass

    def get_access_token(self, scope: Optional[str] = None) -> str:
        """
         Not supported in local client.
        :param scope:
        :return:
        """
        pass

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Not supported in local client.
        :param path:
        :param set_env:
        :return:
        """
        pass

    def connect_account(self):
        """
         Not supported in local client.
        :return:
        """
        pass