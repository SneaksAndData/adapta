"""
 Abstraction for storage operations.
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
from typing import Optional, Dict, Type, TypeVar, Iterator, Callable

from adapta.security.clients import AuthenticationClient
from adapta.storage.models.base import DataPath
from adapta.storage.models.format import SerializationFormat


T = TypeVar("T")  # pylint: disable=C0103


class StorageClient(ABC):
    """
    Base storage operations for all backends.
    """

    def __init__(self, *, base_client: AuthenticationClient):
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

    @abstractmethod
    def list_blobs(
        self,
        blob_path: DataPath,
        filter_predicate: Optional[Callable[[...], bool]] = None,
    ) -> Iterator[DataPath]:
        """
        Lists blobs in blob_path

        :param blob_path: Blob path as DataPath object
        :param filter_predicate: Take only blobs that match a supplied predicate.
        :return: An iterator of DataPaths to blobs
        """

    @abstractmethod
    def read_blobs(
        self,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        filter_predicate: Optional[Callable[[...], bool]] = None,
    ) -> Iterator[T]:
        """
         Reads data under provided path into the given format.

        :param blob_path: Path to blob(s).
        :param serialization_format: Format to deserialize blobs into.
        :param filter_predicate: Take only blobs that match a supplied predicate.
        :return: An iterator over deserialized blobs
        """

    @abstractmethod
    def download_blobs(
        self,
        blob_path: DataPath,
        local_path: str,
        threads: Optional[int] = None,
        filter_predicate: Optional[Callable[[...], bool]] = None,
    ) -> None:
        """
         Reads data under provided path into the given format.

         Be aware that this method does not validate file checksums or download integrity.
         When using threads, download failures will or will not be retried based
         on underlying implementation of a http retry policy.

        :param blob_path: Path to blob(s).
        :param local_path: Path to download blobs to.
        :param threads: Optional number of threads to use when downloading.
                        If not provided, files will be downloaded sequentially.
        :param filter_predicate: Take only blobs that match a supplied predicate.
                 This function accepts an object that describes a cloud blob (BlobProperties for Azure, S3Object for AWS etc.).
                 Client implementations will define the exact parameter type to use.
        :return:
        """

    @abstractmethod
    def copy_blob(self, blob_path: DataPath, target_blob_path: DataPath, doze_period_ms: int) -> None:
        """
        Copy blob at `blob_path` to `target_blob_path`

        :param blob_path: Path to source blob.
        :param target_blob_path: Path to target blob.
        :param doze_period_ms: number of ms to doze between polling the status of the copy.
        """

    @classmethod
    @abstractmethod
    def for_storage_path(cls, path: str) -> "StorageClient":
        """
        Create client instance that can operate on the provided path
        """
