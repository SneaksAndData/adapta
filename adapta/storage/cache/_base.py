"""
  Generic key-value cache.
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
from datetime import timedelta
from typing import Any, List, Optional


class KeyValueCache(ABC):
    """
    Abstract key-value cache store.
    """

    @abstractmethod
    def get(self, key: str, is_map=False) -> Any:
        """
          Retrieve a value associated with the key.

        :param key: A cached key.
        :param is_map: If a value associated with a key is a map where individual fields were added with `include`.
        :return: A value.
        """

    @abstractmethod
    def exists(self, key: str, attribute: Optional[str] = None) -> bool:
        """
          Checks if a cache key is present. If an attribute is provided, should also check
          if a value possesses this attributes.

        :param key: A cache key.
        :param attribute: Optional value attribute.
        :return:
        """

    @abstractmethod
    def multi_exists(self, keys: List[str]) -> bool:
        """
         Checks if all keys exist

        :param keys: Keys to check.
        :return:
        """

    @abstractmethod
    def multi_get(self, keys: List[str]) -> List[Any]:
        """
          Reads multiple keys in a single call.

        :param keys: A list of keys to retrieve values for.
        :return: A list of associated values.
        """

    @abstractmethod
    def set(
        self,
        key: str,
        value: Any,
        expires_after=timedelta(seconds=60),
        return_old_value=False,
    ) -> Any:
        """
          Saves or updates a key-value pair to this cache. Supported value types differ based on implementation.

          Update is performed by replacing a value with a supplied one.

        :param key: A key.
        :param value: A value.
        :param expires_after: Period after which the value expires.
        :param return_old_value: Return old value if it was present.
        :return: A value that has been set or replaced.
        """

    @abstractmethod
    def evict(self, key: str) -> None:
        """
          Removes a key from this cache.

        :param key: A key to remove.
        :return:
        """

    @abstractmethod
    def include(self, key: str, attribute: str, value: Any) -> Any:
        """
         Adds an attribute to a map stored at a specified key. Returns the supplied value.

        :param key: A cached key.
        :param attribute: An attribute to add.
        :param value: A value associated with the attribute.
        :param expires_after: Period after which the value expires.
        :return:
        """

    @abstractmethod
    def set_expiration(self, key: str, expires_after: timedelta) -> None:
        """
          Update
        :param key: A cached key.
        :param expires_after: Period after which the value expires.
        :return:
        """
