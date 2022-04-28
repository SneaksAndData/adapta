"""
 Abstraction for storage operations.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Type, TypeVar

import pandas

from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath
from proteus.storage.models.format import SerializationFormat, DataFrameParquetSerializationFormat


T = TypeVar('T')  # pylint: disable=C0103


class StorageClient(ABC):
    """
     Base storage operations for all backends.
    """
    def __init__(self, *, base_client: ProteusClient):
        self._base_client = base_client

    @abstractmethod
    def save_bytes_as_blob(
        self,
        data_bytes: bytes,
        blob_path: DataPath,
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = False
    ) -> None:
        """
         Saves byte array to a blob.

        :param data_bytes: Bytes to save.
        :param blob_path: Blob path in DataPath notation.
        :param metadata: Optional blob tags or metadata to attach.
        :param overwrite: whether a blob should be overwritten or an exception thrown if it already exists.
        :return:
        """

    @abstractmethod
    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        """
         Generates a URL which can be used to download this blob.

        :param blob_path:
        :param kwargs:
        :return:
        """

    def save_data_as_blob(
        self,
        data: T,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = False,
    ) -> None:
        # pylint: disable=R0913
        """
         Saves dataframe with the given serialization format.

        :param data: Data to save.
        :param blob_path: Blob path in DataPath notation.
        :param metadata: Optional blob tags or metadata to attach.
        :param overwrite: whether a blob should be overwritten or an exception thrown if it already exists.
        :param serialization_format: The serialization format.
            The type (T) of the serialization format must be compatible with the provided data.
        :return:
        """
        bytes_ = serialization_format().serialize(data)
        self.save_bytes_as_blob(
            blob_path=blob_path,
            metadata=metadata,
            overwrite=overwrite,
            data_bytes=bytes_
        )
