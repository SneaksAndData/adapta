"""
 Abstraction for storage operations.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict

from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath


class StorageClient(ABC):
    """
     Base storage operations for all backends.
    """
    def __init__(self, *, base_client: ProteusClient):
        self._base_client = base_client

    @abstractmethod
    def save_bytes_as_blob(self, data_bytes: bytes, blob_path: DataPath, metadata: Optional[Dict[str, str]] = None, overwrite = False):
        """
         Saves byte array to a blob.

        :param data_bytes: Bytes to save.
        :param blob_path: Blob path in DataPath notation.
        :param metadata: Optional blob tags or metadata to attach.
        :param overwrite: whether a blob should be overwritten or an exception thrown if it already exists.
        :return:
        """

    @abstractmethod
    def get_blob_uri(self, blob_path: DataPath, **kwargs):
        """
         Generates a URL which can be used to download this blob.

        :param blob_path:
        :param kwargs:
        :return:
        """
