"""
 Abstraction for secret storage operations.
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
from typing import Union, Dict, Iterable, Any

from adapta.security.clients import AuthenticationClient


class SecretStorageClient(ABC):
    """
    Base secret storage operations for all backends.
    """

    def __init__(self, *, base_client: AuthenticationClient):
        self._base_client = base_client

    @abstractmethod
    def read_secret(self, storage_name: str, secret_name: str) -> Union[bytes, str, Dict[str, str]]:
        """
          Reads a secret from the specified storage.

        :param storage_name: Name of a storage service hosting the secret.
        :param secret_name: Name of the secret
        :return:
        """

    @abstractmethod
    def create_secret(
        self,
        storage_name: str,
        secret_name: str,
        secret_value: Union[str, Dict[str, str]],
        b64_encode=False,
    ) -> None:
        """
          Creates a plain text secret in a specified storage.

        :param storage_name: Name of a storage service hosting the secret.
        :param secret_name: Name of the secret
        :param secret_value: Secret value as plain text.
        :param b64_encode: Whether the value should be b64-encoded
        :return:
        """

    @abstractmethod
    def list_secrets(self, storage_name: str, name_prefix: str) -> Iterable[Any]:
        """
          List secrets with in specified storage.

        :param storage_name: Name of a storage service hosting the secret.
        :param name_prefix: Prefix for filtering secrets
        :return:
        """
