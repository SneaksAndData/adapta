"""
 Base client for all infrastructure providers.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict

from pyarrow.fs import PyFileSystem
from proteus.storage.models.base import DataPath


class ProteusClient(ABC):
    """
     Base functionality all infrastructure providers must implement.
    """

    @abstractmethod
    def get_credentials(self):
        """
         Authentication credentials getter.
        :return:
        """

    @abstractmethod
    def get_access_token(self, scope: Optional[str] = None) -> str:
        """
         If a provider uses OAuth2, it must implement this method to allow fetching access tokens on the fly.

        :param scope: OAuth2 access scope.
        :return: Access token (JWT).
        """

    @abstractmethod
    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Optional method to create authenticated session for the provided path.

        :param path: Data path to authenticate.
        :param set_env: if set, saves credentials in provider-specific environment variables.
        :return: Environment variables with credentials, if any.
        """

    @abstractmethod
    def connect_account(self):
        """
         Connects infrastructure provider account, usually by setting specific environment variables.

        :return:
        """

    @abstractmethod
    def get_pyarrow_filesystem(self, path: DataPath) -> PyFileSystem:
        """
        Returns a `PyFileSystem` object that's authenticated for the provided path

        :param path: Data path to authenticate.
        :return:
        """
