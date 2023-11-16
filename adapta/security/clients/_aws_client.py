"""
 Amazon Web Services implementation of AuthenticationClient.
"""
#  Copyright (c) 2023. ECCO Sneaks & Data
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

import os
from typing import Optional, Dict

import boto3
from pyarrow.filesystem import FileSystem

from adapta.security.clients._base import AuthenticationClient
from adapta.storage.models.base import DataPath


class AwsClient(AuthenticationClient):
    """
    AWS Credentials provider for various AWS resources.
    """

    def __init__(self):
        self._session = None

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
         Optional method to create authenticated session for the provided path.

        :param path: Data path to authenticate.
        :param set_env: Not used in AWS client since only environment credentials are supported
        :return: Environment variables with credentials, if any.
        """
        self._session = self._initialize_from_environment()
        return {
            "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
            "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
            "AWS_REGION": os.environ["AWS_REGION"],
        }

    def connect_account(self):
        """
         Not used in AWS.
        :return:
        """

    def get_pyarrow_filesystem(self, path: DataPath, connection_options: Optional[Dict[str, str]] = None) -> FileSystem:
        raise ValueError("Not supported  in AwsClient")

    @staticmethod
    def _initialize_from_environment():
        if "AWS_SECRET_ACCESS_KEY" not in os.environ:
            raise ValueError("AWS_SECRET_ACCESS_KEY must be set")
        if "AWS_ACCESS_KEY_ID" not in os.environ:
            raise ValueError("AWS_ACCESS_ID must be set")
        if "AWS_REGION" not in os.environ:
            raise ValueError("AWS_REGION must be set")
        return boto3.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_REGION"],
        )
