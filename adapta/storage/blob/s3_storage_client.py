"""
 Storage Client implementation for AWS S3 Cloud.
"""
#  Copyright (c) 2023. ECCO Sneaks & Data
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
from adapta.storage.models.aws import cast_path
from adapta.storage.models.base import DataPath
from adapta.storage.models.format import SerializationFormat

T = TypeVar("T")  # pylint: disable=C0103


class S3StorageClient(StorageClient, ABC):
    """
    S3 Storage Client.
    """

    def __init__(self, *, base_client: AwsClient, bucket_name: str):
        super().__init__(base_client=base_client)
        if base_client.session is None:
            raise ValueError("AwsClient.connect_storage should be called before accessing S3StorageClient")
        self.s3 = base_client.session.resource("s3")
        self.bucket = self.s3.Bucket(bucket_name)
        self.bucket_name = bucket_name

    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        """
         Generates a URL which can be used to download this blob.

        :param blob_path:
        :param kwargs:
        :return:
        """
        return cast_path(blob_path).to_uri()

    def blob_exists(self, blob_path: DataPath) -> bool:
        """Checks if blob located at blob_path exists

        :param blob_path: Path to blob

        :return: Boolean indicator of blob existence
        """
        return any(self.bucket.objects.filter(Prefix=cast_path(blob_path).path))

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
        bytes_ = serialization_format().serialize(data)
        self.bucket.put_object(cast_path(blob_path).path, bytes_)

    def delete_blob(self, blob_path: DataPath) -> None:
        """
        Deletes blob at blob_path

        :param blob_path: Blob path as DataPath object
        """
        self.bucket.Object(blob_path.path).delete()

    def list_blobs(
        self, blob_path: DataPath, filter_predicate: Optional[Callable[[...], bool]] = None
    ) -> Iterator[DataPath]:
        """
        Lists blobs in blob_path

        :param blob_path: Blob path as DataPath object
        :param filter_predicate: Take only blobs that match a supplied predicate.
        :return: An iterator of DataPaths to blobs
        """
        return self.bucket.objects.all()

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
        for blob in self.bucket.objects.filter(Prefix=cast_path(blob_path).path):
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
        Not implemented in S3 Client
        """
        raise NotImplemented("Not implemented in S3StorageClient")

    def copy_blob(self, blob_path: DataPath, target_blob_path: DataPath, doze_period_ms: int) -> None:
        """
        Copy blob at `blob_path` to `target_blob_path`

        :param blob_path: Path to source blob.
        :param target_blob_path: Path to target blob.
        :param doze_period_ms: number of ms to doze between polling the status of the copy.
        """
        copy_source = {"Bucket": self.bucket_name, "Key": blob_path.path}
        self.bucket.Object(cast_path(blob_path).path).copy(copy_source, cast_path(target_blob_path).path)
