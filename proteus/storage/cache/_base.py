from abc import ABC, abstractmethod
from datetime import timedelta
from typing import Any, List


class KeyValueCache(ABC):
    """
      Abstract key-value cache store.
    """

    @abstractmethod
    def get(self, key: str) -> Any:
        """
          Retrieve a value associated with the key.

        :param key: A cached key.
        :return: A value.
        """

    @abstractmethod
    def multi_get(self, keys: List[str]) -> List[Any]:
        """
          Reads multiple keys in a single call.

        :param keys: A list of keys to retrieve values for.
        :return: A list of associated values.
        """

    @abstractmethod
    def set(self, key: str, value: Any, expires_after=timedelta(seconds=60)):
        """
          Saves or updates a key-value pair to this cache. Supported value types differ based on implementation.

          Update is performed by replacing a value with a supplied one.

        :param key: A key.
        :param value: A value.
        :param expires_after: Period after which the value expires
        :return:
        """
