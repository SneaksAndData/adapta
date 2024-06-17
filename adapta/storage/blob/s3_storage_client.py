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
import os
import time
from typing import Optional, Callable, Type, Iterator, Dict, TypeVar, final

from adapta.security.clients import AwsClient
from adapta.storage.blob.base import StorageClient
from adapta.storage.exceptions import StorageClientError
from adapta.storage.models import parse_data_path
from adapta.storage.models.aws import cast_path
from adapta.storage.models.base import DataPath
from adapta.storage.models.format import SerializationFormat

T = TypeVar("T")  # pylint: disable=C0103


@final
class S3StorageClient(StorageClient):
    """
    S3 Storage Client.
    """

    def __init__(self, *, base_client: AwsClient, endpoint_url: Optional[str] = None):
        super().__init__(base_client=base_client)
        if base_client.session is None:
            raise ValueError("AwsClient.initialize_session should be called before accessing S3StorageClient")
        endpoint_url = endpoint_url or base_client.endpoint
        self._s3_resource = base_client.session.resource("s3", endpoint_url=endpoint_url)

    def get_blob_uri(self, blob_path: DataPath) -> str:
        """Returns the URI for a blob in S3 storage.

        :param blob_path: Path to blob

        :return: The URI for the given blob path
        """
        s3_path = cast_path(blob_path)
        return f"s3://{s3_path.bucket}/{s3_path.path}"

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
        :param overwrite: Whether a blob should be overwritten or an exception thrown if it already exists.
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
        Lists blobs in S3 storage.
    
        :param blob_path: Path to blob
        :param filter_predicate: Optional callable to filter blobs
    
        :return: An iterator over a list of the blobs in the S3 storage
        """
        s3_path = cast_path(blob_path)
        response = (
            self._s3_resource.meta.client.list_objects(Bucket=s3_path.bucket, Prefix=s3_path.path, Delimiter='/'))
        if 'Contents' in response:
            for blob in response['Contents']:
                if filter_predicate is None or filter_predicate(blob):
                    yield blob

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
        Downloads blobs from S3 storage to a local path.

        :param blob_path: Path to blob
        :param local_path: Local path to download the blobs to
        :param threads: Number of threads to use for the download
        :param filter_predicate: Optional callable to filter blobs
        :return:
        """
        s3_path = cast_path(blob_path)
        try:
            blobs = self._s3_resource.Bucket(s3_path.bucket).objects.filter(Prefix=s3_path.path)
            for blob in blobs:
                if filter_predicate is None or filter_predicate(blob):
                    local_file_path = f"{local_path}/{blob.key}"
                    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                    self._s3_resource.meta.client.download_file(s3_path.bucket, blob.key, f"{local_path}/{blob.key}")
        except StorageClientError as error:
            print(f"Error downloading blob: {error}")

    def copy_blob(self, blob_path: DataPath, target_blob_path: DataPath, doze_period_ms: int) -> None:
        """
        Copies a blob from one location to another in S3 storage.

        :param blob_path: Path to the source blob
        :param target_blob_path: Path to the target location
        :param doze_period_ms: Time to sleep between operations in milliseconds
        """
        source_s3_path = cast_path(blob_path)
        target_s3_path = cast_path(target_blob_path)

        source_objects = self._s3_resource.Bucket(source_s3_path.bucket).objects.filter(Prefix=source_s3_path.path)

        for source_object in source_objects:
            # If the source path is a directory, construct the target object key
            # If the source path is a file, the target object key is the target path
            if source_s3_path.path == source_object.key:
                target_object_key = target_s3_path.path
            else:
                target_object_key = source_object.key.replace(source_s3_path.path, target_s3_path.path, 1)

            copy_source = {
                'Bucket': source_s3_path.bucket,
                'Key': source_object.key
            }

            try:
                self._s3_resource.meta.client.copy(copy_source, target_s3_path.bucket, target_object_key)
                time.sleep(doze_period_ms / 1000.0)
            except StorageClientError as error:
                print(f"Error copying object: {error}")

    @classmethod
    def for_storage_path(cls, path: str) -> "S3StorageClient":
        """
        Generate client instance that can operate on the provided path. Always uses EnvironmentCredentials/
        """
        _ = cast_path(parse_data_path(path))
        return cls(base_client=AwsClient())
