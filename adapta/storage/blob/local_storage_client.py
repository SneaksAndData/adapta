"""
 Storage Client implementation for a regular filesystem.
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

import os.path
import shutil

from typing import final, Optional, Callable, Type, Iterator, Dict

from adapta.security.clients import LocalClient
from adapta.storage.blob.base import StorageClient, T
from adapta.storage.models import DataPath, LocalPath, parse_data_path
from adapta.storage.models.format import SerializationFormat


@final
class LocalStorageClient(StorageClient):
    """
    Local Storage Client, primarily for unit tests.
    """

    @classmethod
    def create(cls, auth: StorageClient, endpoint_url: Optional[str] = None):
        raise NotImplementedError("Not supported by this client")

    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        return cast_path(blob_path).path

    def blob_exists(self, blob_path: DataPath) -> bool:
        return os.path.isfile(cast_path(blob_path).path)

    def save_data_as_blob(
        self,
        data: T,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = False,
    ) -> None:
        bytes_ = serialization_format().serialize(data)
        file_path = cast_path(blob_path).path

        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "wb") as target:
            target.write(bytes_)

    def delete_blob(self, blob_path: DataPath) -> None:
        os.remove(cast_path(blob_path).path)

    def list_blobs(
        self, blob_path: DataPath, filter_predicate: Optional[Callable[[...], bool]] = None
    ) -> Iterator[DataPath]:
        for blob in os.listdir(cast_path(blob_path).path):
            yield LocalPath(path=blob)

    def read_blobs(
        self,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        filter_predicate: Optional[Callable[[...], bool]] = None,
    ) -> Iterator[T]:
        dir_path = cast_path(blob_path).path
        for blob in os.listdir(dir_path):
            with open(os.path.join(dir_path, blob), "rb") as blob_file:
                yield serialization_format().deserialize(blob_file.read())

    def download_blobs(
        self,
        blob_path: DataPath,
        local_path: str,
        threads: Optional[int] = None,
        filter_predicate: Optional[Callable[[...], bool]] = None,
    ) -> None:
        raise NotImplementedError("Not supported by this client")

    def copy_blob(self, blob_path: DataPath, target_blob_path: DataPath, doze_period_ms: int = 0) -> None:
        shutil.copyfile(cast_path(blob_path).path, cast_path(target_blob_path).path)

    def upload_blob(self, source_file_path: str, target_file_path: DataPath, doze_period_ms: int) -> None:
        raise NotImplementedError("Not supported by this client")

    @classmethod
    def for_storage_path(cls, path: str) -> "StorageClient":
        _ = cast_path(parse_data_path(path))
        return cls(base_client=LocalClient())


def cast_path(blob_path: DataPath) -> LocalPath:
    """
     Type cast from DataPath to LocalPath

    :param blob_path: DataPath
    :return: LocalPath
    """
    assert isinstance(blob_path, LocalPath), "Only LocalPath paths are supported by this client."

    return blob_path
