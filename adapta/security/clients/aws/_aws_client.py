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

from typing import Optional, Dict, final, Callable

import boto3
from boto3.session import Session
from pyarrow.fs import FileSystem

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
        self._credentials = aws_credentials

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

    def get_credentials(self) -> Optional[AccessKeyCredentials]:
        """
        Returns configured credentials (if any)
        """
        return self._credentials

    def get_access_token(self, scope: Optional[str] = None) -> str:
        """
        Not used in AWS.
        """

    def connect_storage(self, path: DataPath, set_env: bool = False) -> Optional[Dict]:
        """
        Configures the necessary storage options to be used to connect the AWS client for Delta Lake operations.
        :return: All need storage options to set up Delta Lake storage client.
        """
        return {
            "AWS_ACCESS_KEY_ID": self._credentials.access_key_id,
            "AWS_SECRET_ACCESS_KEY": self._credentials.access_key,
            "AWS_REGION": self._credentials.region,
            "AWS_ENDPOINT_URL": "" if self._credentials.endpoint is None else self._credentials.endpoint,
        }

    def connect_account(self):
        """
         Not used in AWS.
        :return:
        """

    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        """
        Not supported in AwsClient.
        :return:
        """

    def initialize_session(self, session_callable: Optional[Callable[[], Session]] = None) -> "AwsClient":
        """
        Initializes the session by custom session function or a default one if no function is provided."
        :return: AwsClient with established session.
        """
        if self._session is not None:
            return self

        if session_callable is None:
            session_callable = self._default_aws_session

        self._session = session_callable()

        return self

    def _default_aws_session(self) -> Session:
        """
        Initializes the session using stored AWS credentials. If not, retrieves them from environment variables."
        """
        if self._credentials is None:
            self._credentials = EnvironmentAwsCredentials()

        return boto3.Session(
            aws_access_key_id=self._credentials.access_key_id,
            aws_secret_access_key=self._credentials.access_key,
            region_name=self._credentials.region,
            aws_session_token=self._credentials.session_token,
        )
