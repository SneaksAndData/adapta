"""
 Storage Client implementation for Azure Cloud.
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
from datetime import datetime, timedelta
from functools import partial
import signal
from threading import Thread
from typing import Union, Optional, Dict, Type, TypeVar, Iterator, List, Callable, final

from azure.core.paging import ItemPaged
from azure.storage.blob import (
    BlobServiceClient,
    BlobSasPermissions,
    BlobClient,
    generate_blob_sas,
    BlobProperties,
    ExponentialRetry,
    ContainerClient,
)

from adapta.storage.blob.base import StorageClient
from adapta.security.clients import AzureClient
from adapta.storage.models import parse_data_path
from adapta.storage.models.azure import AdlsGen2Path, WasbPath, cast_path
from adapta.storage.models.base import DataPath
from adapta.storage.models.format import SerializationFormat
from adapta.utils import chunk_list, doze

T = TypeVar("T")  # pylint: disable=C0103


@final
class AzureStorageClient(StorageClient):
    """
    Azure Storage (Blob and ADLS) Client.
    """

    def __init__(self, *, base_client: AzureClient, path: Union[AdlsGen2Path, WasbPath], implicit_login=True):
        super().__init__(base_client=base_client)

        # overrides default ExponentialRetry
        # config.retry_policy = kwargs.get("retry_policy") or ExponentialRetry(**kwargs)
        retry_policy = ExponentialRetry(initial_backoff=5, increment_base=3, retry_total=15)

        if implicit_login:
            self._blob_service_client: BlobServiceClient = BlobServiceClient(
                account_url=WasbPath.from_adls2_path(path).base_uri()
                if isinstance(path, AdlsGen2Path)
                else path.base_uri(),
                credential=self._base_client.get_credentials(),
                retry_policy=retry_policy,
            )
            self._storage_options = None
        else:
            self._storage_options = self._base_client.connect_storage(path)
            connection_string = (
                f"DefaultEndpointsProtocol=https;"
                f"AccountName={self._storage_options['AZURE_STORAGE_ACCOUNT_NAME']};"
                f"AccountKey={self._storage_options['AZURE_STORAGE_ACCOUNT_KEY']};"
                f"BlobEndpoint=https://{self._storage_options['AZURE_STORAGE_ACCOUNT_NAME']}.blob.core.windows.net/;"
            )
            self._blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(
                connection_string, retry_policy=retry_policy
            )

    @classmethod
    def create(cls, auth: AzureClient, endpoint_url: Optional[str] = None):
        """
         Not used in Azure.
        :return:
        """
        raise NotImplementedError("Not implemented in AzClient")

    @classmethod
    def for_storage_path(cls, path: str) -> "AzureStorageClient":
        """
        Generate client instance that can operate on the provided path
        """
        azure_path = cast_path(parse_data_path(path))
        return cls(base_client=AzureClient(), path=azure_path)

    def _get_blob_client(self, blob_path: DataPath) -> BlobClient:
        azure_path = cast_path(blob_path)

        assert (
            azure_path.account == self._blob_service_client.account_name
        ), "Path provided is in another storage account and cannot be used."

        return self._blob_service_client.get_blob_client(
            container=azure_path.container,
            blob=azure_path.path,
        )

    def _get_container_client(self, blob_path: DataPath) -> ContainerClient:
        azure_path = cast_path(blob_path)

        assert (
            azure_path.account == self._blob_service_client.account_name
        ), "Path provided is in another storage account and cannot be used."

        return self._blob_service_client.get_container_client(container=azure_path.container)

    def save_data_as_blob(  # pylint: disable=R0913,R0801
        self,
        data: T,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        metadata: Optional[Dict[str, str]] = None,
        overwrite: bool = False,
    ) -> None:
        bytes_ = serialization_format().serialize(data)
        self._get_blob_client(blob_path).upload_blob(bytes_, metadata=metadata, overwrite=overwrite)

    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        blob_client = self._get_blob_client(blob_path)
        azure_path = cast_path(blob_path)

        base_call = partial(
            generate_blob_sas,
            blob_name=azure_path.path,
            container_name=azure_path.container,
            account_name=azure_path.account,
            permission=kwargs.get("permission", BlobSasPermissions(read=True)),
            expiry=kwargs.get("expiry", datetime.utcnow() + timedelta(hours=1)),
        )

        sas_token = (
            base_call(
                account_key=self._storage_options["AZURE_STORAGE_ACCOUNT_KEY"],
            )
            if self._storage_options
            else base_call(
                user_delegation_key=self._blob_service_client.get_user_delegation_key(
                    key_start_time=datetime.utcnow() - timedelta(minutes=1),
                    key_expiry_time=kwargs.get("expiry", datetime.utcnow() + timedelta(hours=1)),
                ),
            )
        )

        sas_uri = f"{blob_client.url}?{sas_token}"
        return sas_uri

    def blob_exists(self, blob_path: DataPath) -> bool:
        return self._get_blob_client(blob_path).exists()

    def _list_blobs(self, blob_path: DataPath) -> (ItemPaged[BlobProperties], Union[AdlsGen2Path, WasbPath]):
        azure_path = cast_path(blob_path)

        return (
            self._get_container_client(azure_path).list_blobs(name_starts_with=blob_path.path),
            azure_path,
        )

    def read_blobs(
        self,
        blob_path: DataPath,
        serialization_format: Type[SerializationFormat[T]],
        filter_predicate: Optional[Callable[[BlobProperties], bool]] = None,
    ) -> Iterator[T]:
        blobs_on_path, azure_path = self._list_blobs(blob_path)

        for blob in blobs_on_path:
            if (filter_predicate or (lambda _: True))(blob):
                blob_data: bytes = (
                    self._blob_service_client.get_blob_client(
                        container=azure_path.container,
                        blob=blob.name,
                    )
                    .download_blob()
                    .readall()
                )

                yield serialization_format().deserialize(blob_data)

    def download_blob(
        self,
        blob_path: DataPath,
        local_path: str,
    ) -> None:
        """Download a file from ADLS"""
        azure_path = cast_path(blob_path)

        os.makedirs(local_path, exist_ok=True)
        with open(os.path.join(local_path, azure_path.path.split("/")[-1]), "wb") as downloaded_blob:
            downloaded_blob.write(
                self._blob_service_client.get_blob_client(
                    container=azure_path.container,
                    blob=azure_path.path,
                )
                .download_blob()
                .readall()
            )

    def download_blobs(
        self,
        blob_path: DataPath,
        local_path: str,
        threads: Optional[int] = None,
        filter_predicate: Optional[Callable[[BlobProperties], bool]] = None,
    ) -> None:
        def download_blob(blob: BlobProperties, container: str) -> None:
            write_path = os.path.join(local_path, blob.name)
            if blob.size == 0:
                os.makedirs(write_path, exist_ok=True)
            else:
                with open(write_path, "wb") as downloaded_blob:
                    downloaded_blob.write(
                        self._blob_service_client.get_blob_client(
                            container=container,
                            blob=blob.name,
                        )
                        .download_blob()
                        .readall()
                    )

        def download_blob_list(blob_list: List[BlobProperties], container: str) -> None:
            for blob_from_list in blob_list:
                if blob_from_list:
                    download_blob(blob_from_list, container)

        os.makedirs(local_path, exist_ok=True)
        blobs_on_path, azure_path = self._list_blobs(blob_path)

        if not threads:
            for blob_on_path in blobs_on_path:
                if (filter_predicate or (lambda _: True))(blob_on_path):
                    download_blob(blob_on_path, azure_path.container)
        else:
            blobs = [blob for blob in list(blobs_on_path) if (filter_predicate or (lambda _: True))(blob)]
            blob_dirs = [blob_dir for blob_dir in blobs if blob_dir.size == 0]
            blob_files = [blob_dir for blob_dir in blobs if blob_dir.size > 0]

            # we need to create dirs in advance to avoid locking threads

            for blob_dir in blob_dirs:
                os.makedirs(os.path.join(local_path, blob_dir.name), exist_ok=True)

            blob_lists: List[List[BlobProperties]] = chunk_list(blob_files, threads)
            thread_list = [
                Thread(target=download_blob_list, args=(blob_list, azure_path.container)) for blob_list in blob_lists
            ]
            for download_thread in thread_list:
                download_thread.start()
            for download_thread in thread_list:
                download_thread.join()

    def list_blobs(
        self,
        blob_path: DataPath,
        filter_predicate: Optional[Callable[[BlobProperties], bool]] = lambda blob: blob.size != 0,  # Skip folders
    ) -> Iterator[DataPath]:
        blobs_on_path, azure_path = self._list_blobs(blob_path)

        for blob in blobs_on_path:
            if (filter_predicate or (lambda _: True))(blob):
                yield AdlsGen2Path(
                    account=azure_path.account,
                    container=azure_path.container,
                    path=blob.name,
                )

    def delete_blob(
        self,
        blob_path: DataPath,
    ) -> None:
        azure_path = cast_path(blob_path)

        self._get_container_client(azure_path).delete_blob(blob_path.path)

    def copy_blob(self, blob_path: DataPath, target_blob_path: DataPath, doze_period_ms=1000) -> None:
        source_url = self.get_blob_uri(blob_path)
        self._get_blob_client(target_blob_path).start_copy_from_url(source_url)

        def abort_query(_signal, _handler):
            target_blob = self._get_blob_client(target_blob_path).get_blob_properties()
            self._get_blob_client(target_blob_path).abort_copy(target_blob)

        signal.signal(signal.SIGINT, abort_query)

        while True:
            copy_status = self._get_blob_client(target_blob_path).get_blob_properties().copy
            if copy_status.status == "success":
                break

            if copy_status.status != "pending":
                raise RuntimeError(
                    f"Copy of file {blob_path.to_hdfs_path()} to {target_blob_path.to_hdfs_path()} failed with error:\n{copy_status.status}, {copy_status.status_description}"
                )

            doze(doze_period_ms)

    def upload_blob(self, source_file_path: str, target_file_path: DataPath, doze_period_ms: int) -> None:
        """
         Not used in Azure.
        :return:
        """
        raise NotImplementedError("Not implemented in AzClient")
