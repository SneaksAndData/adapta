"""
 Storage Client implementation for Azure Cloud.
"""
import os.path
from datetime import datetime, timedelta
from threading import Thread
from typing import Union, Optional, Dict, Type, TypeVar, Iterator, List, Callable

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

from proteus.storage.blob.base import StorageClient
from proteus.security.clients import AzureClient
from proteus.storage.models.azure import AdlsGen2Path, WasbPath, cast_path
from proteus.storage.models.base import DataPath
from proteus.storage.models.format import SerializationFormat
from proteus.utils import chunk_list

T = TypeVar("T")  # pylint: disable=C0103


class AzureStorageClient(StorageClient):
    """
    Azure Storage (Blob and ADLS) Client.
    """

    def __init__(self, *, base_client: AzureClient, path: Union[AdlsGen2Path, WasbPath]):
        super().__init__(base_client=base_client)
        self._storage_options = self._base_client.connect_storage(path)
        connection_string = (
            f"DefaultEndpointsProtocol=https;"
            f"AccountName={self._storage_options['AZURE_STORAGE_ACCOUNT_NAME']};"
            f"AccountKey={self._storage_options['AZURE_STORAGE_ACCOUNT_KEY']};"
            f"BlobEndpoint=https://{self._storage_options['AZURE_STORAGE_ACCOUNT_NAME']}.blob.core.windows.net/;"
        )

        # overrides default ExponentialRetry
        # config.retry_policy = kwargs.get("retry_policy") or ExponentialRetry(**kwargs)
        self._blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(
            connection_string, retry_policy=ExponentialRetry(initial_backoff=5, increment_base=3, retry_total=15)
        )

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

        sas_token = generate_blob_sas(
            blob_name=azure_path.path,
            container_name=azure_path.container,
            account_name=azure_path.account,
            permission=kwargs.get("permission", BlobSasPermissions(read=True)),
            expiry=kwargs.get("expiry", datetime.utcnow() + timedelta(hours=1)),
            account_key=self._storage_options["AZURE_STORAGE_ACCOUNT_KEY"],
        )

        sas_uri = f"{blob_client.url}?{sas_token}"
        return sas_uri

    def blob_exists(self, blob_path: DataPath) -> bool:
        return self._get_blob_client(blob_path).exists()

    def _list_blobs(self, blob_path: DataPath) -> (ItemPaged[BlobProperties], Union[AdlsGen2Path, WasbPath]):
        azure_path = cast_path(blob_path)

        return self._get_container_client(azure_path).list_blobs(name_starts_with=blob_path.path), azure_path

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
                yield AdlsGen2Path(account=azure_path.account, container=azure_path.container, path=blob.name)

    def delete_blob(
        self,
        blob_path: DataPath,
    ) -> None:
        azure_path = cast_path(blob_path)

        self._get_container_client(azure_path).delete_blob(blob_path.path)
