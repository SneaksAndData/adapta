"""
 Storage Client implementation for AWS S3.
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

from abc import ABC
from typing import Optional, Callable, Type, Iterator, Dict, TypeVar

from adapta.security.clients import AwsClient
from adapta.storage.blob.base import StorageClient
from adapta.storage.exceptions import StorageClientError
from adapta.storage.models import parse_data_path
from adapta.storage.models.aws import cast_path
from adapta.storage.models.base import DataPath
from adapta.storage.models.format import SerializationFormat

T = TypeVar("T")  # pylint: disable=C0103


class S3StorageClient(StorageClient, ABC):
    """
    S3 Storage Client.
    """

    def __init__(self, *, base_client: AwsClient):
        super().__init__(base_client=base_client)
        if base_client.session is None:
            raise ValueError("AwsClient.initialize_session should be called before accessing S3StorageClient")
        self._s3_resource = base_client.session.resource("s3")

    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        """
        Not implemented in S3 Client
        """
        raise NotImplementedError("Not implemented in S3StorageClient")

    def blob_exists(self, blob_path: DataPath) -> bool:
        """Checks if blob located at blob_path exists

        :param blob_path: Path to blob

        :return: Boolean indicator of blob existence
        """
        s3_path = cast_path(blob_path)
        return any(self._s3_resource.Bucket(s3_path.bucket).objects.filter(Prefix=s3_path.path))

    def save_data_as_blob(
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
        if not overwrite:
            if self.blob_exists(blob_path=blob_path):
                raise StorageClientError(
                    f"Blob already exists at path: {blob_path.path}. Please specify overwrite=True if you want to overwrite it."
                )

        s3_path = cast_path(blob_path)
        bytes_ = serialization_format().serialize(data)
        self._s3_resource.Bucket(s3_path.bucket).put_object(Key=s3_path.path, Body=bytes_)

    def delete_blob(self, blob_path: DataPath) -> None:
        """
        Deletes blob at blob_path

        :param blob_path: Blob path as DataPath object
        """
        s3_path = cast_path(blob_path)
        self._s3_resource.Bucket(s3_path.bucket).Object(blob_path.path).delete()

    def list_blobs(
        self, blob_path: DataPath, filter_predicate: Optional[Callable[[...], bool]] = None
    ) -> Iterator[DataPath]:
        """
        Not implemented in S3 Client
        """
        raise NotImplementedError("Not implemented in S3StorageClient")

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
        s3_path = cast_path(blob_path)
        for blob in self._s3_resource.Bucket(s3_path.bucket).objects.filter(Prefix=s3_path.path):
            if filter_predicate is not None and not filter_predicate(blob):
                continue
            yield serialization_format().deserialize(blob.get()["Body"].read())

    def download_blobs(
        self,
        blob_path: DataPath,
        local_path: str,
        threads: Optional[int] = None,
        filter_predicate: Optional[Callable[[...], bool]] = None,
    ) -> None:
        """
        Not yet implemented in S3 Client
        """
        raise NotImplementedError("Not yet implemented in S3StorageClient")

    def copy_blob(self, blob_path: DataPath, target_blob_path: DataPath, doze_period_ms: int) -> None:
        """
        Not implemented in S3 Client
        """
        raise NotImplementedError("Not implemented in S3StorageClient")

    @classmethod
    def for_storage_path(cls, path: str) -> "S3StorageClient":
        """
        Generate client instance that can operate on the provided path. Always uses EnvironmentCredentials/
        """
        _ = cast_path(parse_data_path(path))
        return cls(base_client=AwsClient())
