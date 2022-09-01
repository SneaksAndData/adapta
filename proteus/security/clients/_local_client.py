"""
 Client representing Local infrastructure. Mainly used for unit tests.
"""
from typing import Optional, Dict
from pyarrow.fs import PyFileSystem, FSSpecHandler
from fsspec.implementations.local import LocalFileSystem
from proteus.security.clients._base import ProteusClient
from proteus.storage.models.base import DataPath


class LocalClient(ProteusClient):
    """
     Local mode Proteus Client.
    """

    def get_credentials(self):
        """
         Not supported in local client.
        :return:
        """

    def get_access_token(self, scope: Optional[str] = None) -> str:
        """
         Not supported in local client.
        :param scope:
        :return:
        """

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Not supported in local client.
        :param path:
        :param set_env:
        :return:
        """

    def connect_account(self):
        """
         Not supported in local client.
        :return:
        """

    def get_filesystem(self, path: DataPath) -> PyFileSystem:
        return PyFileSystem(FSSpecHandler(LocalFileSystem()))
