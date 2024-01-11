"""
Contains credentials provider for AWS clients
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
import os
from abc import ABC, abstractmethod


class AccessKeyCredentials(ABC):
    """
    Abstract class that represents credentials for AWS connections
    """

    @property
    @abstractmethod
    def access_key(self) -> str:
        """AWS account access key"""

    @property
    @abstractmethod
    def access_key_id(self) -> str:
        """AWS account access key id"""

    @property
    @abstractmethod
    def region(self) -> str:
        """AWS region"""


class EnvironmentAwsCredentials(AccessKeyCredentials):
    """
    Reads credentials from environment variables
    """

    def __init__(self):
        if "PROTEUS__AWS_SECRET_ACCESS_KEY" not in os.environ:
            raise ValueError("PROTEUS__AWS_SECRET_ACCESS_KEY must be set")
        self._access_key = os.environ["PROTEUS__AWS_SECRET_ACCESS_KEY"]

        if "PROTEUS__AWS_ACCESS_KEY_ID" not in os.environ:
            raise ValueError("PROTEUS__AWS_ACCESS_KEY_ID must be set")
        self._access_key_id = os.environ["PROTEUS__AWS_ACCESS_KEY_ID"]

        if "PROTEUS__AWS_REGION" not in os.environ:
            raise ValueError("PROTEUS__AWS_REGION must be set")
        self._region = os.environ["PROTEUS__AWS_REGION"]

    @property
    def access_key(self) -> str:
        return self._access_key

    @property
    def access_key_id(self) -> str:
        return self._access_key_id

    @property
    def region(self) -> str:
        return self.region


class ExplicitAwsCredentials(AccessKeyCredentials):
    """
    Explicitly passed AWS credentials
    """

    def __init__(self, access_key, access_key_id, region):
        self._access_key = access_key
        self._access_key_id = access_key_id
        self._region = region

    @property
    def access_key(self) -> str:
        return self._access_key

    @property
    def access_key_id(self) -> str:
        return self._access_key_id

    @property
    def region(self) -> str:
        return self._region
