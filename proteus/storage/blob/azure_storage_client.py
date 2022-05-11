"""
 Storage Client implementation for Azure Cloud.
"""
from datetime import datetime, timedelta
from typing import Union, Optional, Dict, Type, TypeVar

from azure.storage.blob import BlobServiceClient, BlobSasPermissions, BlobClient, generate_blob_sas

from proteus.storage.blob.base import StorageClient
from proteus.security.clients import AzureClient
from proteus.storage.models.azure import AdlsGen2Path, WasbPath, cast_path
from proteus.storage.models.base import DataPath
from proteus.storage.models.format import SerializationFormat


T = TypeVar('T')  # pylint: disable=C0103


class AzureStorageClient(StorageClient):
    """
     Azure Storage (Blob and ADLS) Client.
    """
    def __init__(self, *, base_client: AzureClient, path: Union[AdlsGen2Path, WasbPath]):
        super().__init__(base_client=base_client)
        self._storage_options = self._base_client.connect_storage(path)
        connection_string = \
            f"DefaultEndpointsProtocol=https;" \
            f"AccountName={self._storage_options['AZURE_STORAGE_ACCOUNT_NAME']};" \
            f"AccountKey={self._storage_options['AZURE_STORAGE_ACCOUNT_KEY']};" \
            f"BlobEndpoint=https://{self._storage_options['AZURE_STORAGE_ACCOUNT_NAME']}.blob.core.windows.net/;"

        self._blob_service_client: BlobServiceClient = BlobServiceClient.from_connection_string(connection_string)

    def _get_blob_client(self, blob_path: DataPath) -> BlobClient:
        azure_path = cast_path(blob_path)

        return self._blob_service_client.get_blob_client(
            container=azure_path.container,
            blob=azure_path.path,
        )

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
        self._get_blob_client(blob_path).upload_blob(bytes_, metadata=metadata, overwrite=overwrite)

    def get_blob_uri(self, blob_path: DataPath, **kwargs) -> str:
        blob_client = self._get_blob_client(blob_path)
        azure_path = cast_path(blob_path)

        sas_token = generate_blob_sas(
            blob_name=azure_path.path,
            container_name=azure_path.container,
            account_name=azure_path.account,
            permission=kwargs.get('permission', BlobSasPermissions(read=True)),
            expiry=kwargs.get('expiry', datetime.utcnow() + timedelta(hours=1)),
            account_key=self._storage_options['AZURE_STORAGE_ACCOUNT_KEY']
        )

        sas_uri = f'{blob_client.url}?{sas_token}'
        return sas_uri
