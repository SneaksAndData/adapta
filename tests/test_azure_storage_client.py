import os
import tempfile
from unittest.mock import patch, MagicMock

from azure.storage.blob import BlobProperties

from proteus.storage.blob.azure_storage_client import AzureStorageClient
from proteus.storage.models.azure import AdlsGen2Path
from proteus.security.clients import AzureClient


@patch('azure.storage.blob._download.StorageStreamDownloader')
@patch('azure.storage.blob.BlobClient')
@patch('azure.storage.blob.ContainerClient')
@patch('azure.storage.blob.BlobServiceClient')
@patch('proteus.security.clients.AzureClient')
def test_download_blobs(mock_client: MagicMock, mock_blob_service_client: MagicMock, mock_container_client: MagicMock,
                        mock_blob: MagicMock,
                        mock_downloader: MagicMock):
    test_path = f"{tempfile.gettempdir()}/test_download_blobs"
    mock_client_instance: AzureClient = mock_client.return_value
    data_path = AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/folder")
    mock_client_instance.connect_storage.return_value = {
        'AZURE_STORAGE_ACCOUNT_NAME': 'test',
        'AZURE_STORAGE_ACCOUNT_KEY': 'test'
    }

    azure_storage_client = AzureStorageClient(base_client=mock_client_instance, path=data_path)

    azure_storage_client._blob_service_client = mock_blob_service_client
    mock_blob_service_client.get_blob_client.return_value = mock_blob
    mock_blob_service_client.get_container_client.return_value = mock_container_client
    mock_container_client.list_blobs.return_value = [BlobProperties(**{
        "name": f"test{i}",
        "Content-Length": 1
    }) for i in range(10)]

    mock_blob.download_blob = mock_downloader
    mock_downloader.return_value.readall.return_value = b''

    azure_storage_client.download_blobs(blob_path=data_path, local_path=test_path, threads=3)

    file_count = len(os.listdir(test_path))

    assert file_count == 10
