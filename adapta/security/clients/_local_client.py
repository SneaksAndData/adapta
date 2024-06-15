"""
 Client representing Local infrastructure. Mainly used for unit tests.
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

from typing import Optional, Dict
from pyarrow.fs import FileSystem, LocalFileSystem, SubTreeFileSystem
from adapta.security.clients._base import AuthenticationClient
from adapta.storage.models.base import DataPath


class LocalClient(AuthenticationClient):
    """
    Local mode AuthenticationClient.
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

    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        return SubTreeFileSystem(path.path, LocalFileSystem())
