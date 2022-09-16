"""
 Abstraction for storage operations.
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Type, TypeVar, Iterator, Tuple, List

from proteus.security.clients import ProteusClient
from proteus.storage.models.base import DataPath
from proteus.storage.models.format import SerializationFormat


T = TypeVar('T')  # pylint: disable=C0103


class StorageClient(ABC):
    """
     Base storage operations for all backends.
    """
    def __init__(self, *, base_client: ProteusClient):
        self._base_client = base_client

    @abstractmethod
    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        """
         Generates a URL which can be used to download this blob.

        :param blob_path:
        :param kwargs:
        :return:
        """

    @abstractmethod
    def blob_exists(self, blob_path: DataPath) -> bool:
        """Checks if blob located at blob_path exists

        :param blob_path: Path to blob

        :return: Boolean indicator of blob existence
        """

    @abstractmethod
    def save_data_as_blob(  # pylint: disable=R0913,R0801
        self,
        data: T,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = False,
    ) -> None:
        """
         Saves any data with the given serialization format.

        :param data: Data to save.
        :param blob_path: Blob path in DataPath notation.
        :param metadata: Optional blob tags or metadata to attach.
        :param overwrite: whether a blob should be overwritten or an exception thrown if it already exists.
        :param serialization_format: The serialization format.
            The type (T) of the serialization format must be compatible with the provided data.
        :return:
        """

    @abstractmethod
    def delete_blob(
        self,
        blob_path: DataPath,
    ) -> None:
        """
        Deletes blob at blob_path

        :param blob_path: Blob path as DataPath object
        """

    def move_blob(
        self,
        source_blob_path: DataPath,
        destination_blob_path: DataPath,
    ):
        """
        Moves blob from source_blob_path to destination_blob_path

        :param source_blob_path: Path to blob to move
        :param destination_blob_path: Path to blob destination
        """
        self.copy_blob(
            source_blob_path=source_blob_path,
            destination_blob_path=destination_blob_path,
            asynchronous=False,
        )
        self.delete_blob(source_blob_path)

    @abstractmethod
    def copy_blob(
        self,
        source_blob_path: DataPath,
        destination_blob_path: DataPath,
        asynchronous: bool = False,
        time_out_seconds: float = 600,
    ):
        """
        Copies blob from source_blob_path to destination_blob_path

        :param source_blob_path: Path to blob to copy
        :param destination_blob_path: Path to blob destination
        :param asynchronous: Whether to run blob copy asynchronously
        :param time_out_seconds: Maximum seconds to wait for copy operation when asynchronous = False
        """

    @abstractmethod
    def copy_blobs(
        self,
        blob_pairs: List[Tuple[DataPath, DataPath]],
        asynchronous: bool = False,
        time_out_seconds: float = 600.,
    ):
        """
        Asynchronously copies a list of blobs.

        :param blob_pairs: List of tuple of blobs to copy. First value in tuple is the source path while second value is the destination
        :param asynchronous: Whether to run blob copy operations asynchronously
        :param time_out_seconds: Maximum seconds to wait for copy operations
        """

    def move_blobs(
        self,
        blob_pairs: List[Tuple[DataPath, DataPath]],
    ):
        """
        Moves a set of blobs.

        :param blob_pairs: List of tuple of blobs to move. First value in tuple is the source path while second value is the destination
        """
        self.copy_blobs(blob_pairs)
        for source, _ in blob_pairs:
            self.delete_blob(source)

    @abstractmethod
    def list_blobs(
        self,
        blob_path: DataPath,
    ) -> Iterator[DataPath]:
        """
        Lists blobs in blob_path

        :param blob_path: Blob path as DataPath object
        :return: An iterator of DataPaths to blobs
        """

    @abstractmethod
    def read_blobs(self, blob_path: DataPath, serialization_format: Type[SerializationFormat[T]]) -> Iterator[T]:
        """
         Reads data under provided path into the given format.

        :param blob_path: Path to blob(s).
        :param serialization_format: Format to deserialize blobs into.
        :return: An iterator over deserialized blobs
        """

    @abstractmethod
    def download_blobs(self, blob_path: DataPath, local_path: str, threads: Optional[int] = None) -> None:
        """
         Reads data under provided path into the given format.

         Be aware that this method does not validate file checksums or download integrity.
         When using threads, download failures will or will not be retried based
         on underlying implementation of a http retry policy.

        :param blob_path: Path to blob(s).
        :param local_path: Path to download blobs to.
        :param threads: Optional number of threads to use when downloading.
                        If not provided, files will be downloaded sequentially.
        :return:
        """
