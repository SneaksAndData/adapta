"""
 Base client for all infrastructure providers.
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

from abc import ABC, abstractmethod
from typing import Optional, Dict, Callable

from pyarrow.fs import FileSystem
from adapta.storage.models.base import DataPath


class AuthenticationClient(ABC):
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
    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        """
        Returns a `PyFileSystem` object that's authenticated for the provided path

        :param path: Data path to authenticate.
        :param connection_options: Optional connection options to use instead of auto-discovery.
        :return:
        """

    @abstractmethod
    def initialize_session(self, session_callable: Optional[Callable[[], None]] = None) -> "AuthenticationClient":
        """
        Initializes the session by custom session function or a default one if no function is provided.
        """
