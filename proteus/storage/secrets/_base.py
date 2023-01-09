"""
 Abstraction for secret storage operations.
"""

from abc import ABC, abstractmethod
from typing import Union, Dict, Iterable, Any

from proteus.security.clients import ProteusClient


class SecretStorageClient(ABC):
    """
    Base secret storage operations for all backends.
    """

    def __init__(self, *, base_client: ProteusClient):
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
        self, storage_name: str, secret_name: str, secret_value: Union[str, Dict[str, str]], b64_encode=False
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
