"""
 Amazon Web Services implementation of AuthenticationClient.
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

from typing import Optional, Dict, final

import boto3
from pyarrow.filesystem import FileSystem

from adapta.security.clients._base import AuthenticationClient
from adapta.security.clients.aws._aws_credentials import AccessKeyCredentials, EnvironmentAwsCredentials
from adapta.storage.models.base import DataPath


@final
class AwsClient(AuthenticationClient):
    """
    AWS Credentials provider for various AWS resources.
    """

    def __init__(self, aws_credentials: Optional[AccessKeyCredentials] = None):
        self._session = None
        self._credentials = aws_credentials or EnvironmentAwsCredentials()

    @property
    def session(self):
        """
        Returns configured session (if any)
        """
        return self._session

    @classmethod
    def from_base_client(cls, client: AuthenticationClient) -> Optional["AwsClient"]:
        """
         Safe casts AuthenticationClient to AwsClient if type checks out.

        :param client: AuthenticationClient
        :return: AwsClient or None if type does not check out
        """
        return client if isinstance(client, AwsClient) else None

    def get_credentials(self):
        """
         Not used in AWS.
        :return:
        """
        raise NotImplementedError("Not implemented in AwsClient")

    def get_access_token(self, scope: Optional[str] = None) -> str:
        """
         Not used in AWS.
        :return:
        """
        raise NotImplementedError("Authentication with temporary credentials is not supported yet in AwsClient")

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
         Not used in AWS.
        :return:
        """

    def connect_account(self):
        """
         Not used in AWS.
        :return:
        """

    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        raise ValueError("Not supported  in AwsClient")

    def initialize_session(self) -> "AwsClient":
        """
        Initializes session. Should be called before any operations with client
        """
        self._session = boto3.Session(
            aws_access_key_id=self._credentials.access_key_id,
            aws_secret_access_key=self._credentials.access_key,
            region_name=self._credentials.region,
        )
        return self
