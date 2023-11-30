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

import os
import tempfile
from unittest.mock import patch, MagicMock

from azure.storage.blob import BlobProperties

from adapta.storage.blob.azure_storage_client import AzureStorageClient
from adapta.storage.models.azure import AdlsGen2Path
from adapta.security.clients import AzureClient


@patch("azure.storage.blob._download.StorageStreamDownloader")
@patch("azure.storage.blob.BlobClient")
@patch("azure.storage.blob.ContainerClient")
@patch("azure.storage.blob.BlobServiceClient")
@patch("adapta.security.clients.AzureClient")
def test_download_blobs(
    mock_client: MagicMock,
    mock_blob_service_client: MagicMock,
    mock_container_client: MagicMock,
    mock_blob: MagicMock,
    mock_downloader: MagicMock,
):
    test_path = f"{tempfile.gettempdir()}/test_download_blobs"
    mock_client_instance: AzureClient = mock_client.return_value
    data_path = AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/folder")
    mock_client_instance.connect_storage.return_value = {
        "AZURE_STORAGE_ACCOUNT_NAME": "test",
        "AZURE_STORAGE_ACCOUNT_KEY": "test",
    }

    azure_storage_client = AzureStorageClient(base_client=mock_client_instance, path=data_path)

    azure_storage_client._blob_service_client = mock_blob_service_client
    mock_blob_service_client.account_name = "account"
    mock_blob_service_client.get_blob_client.return_value = mock_blob
    mock_blob_service_client.get_container_client.return_value = mock_container_client
    mock_container_client.list_blobs.return_value = [
        BlobProperties(**{"name": f"test{i}", "Content-Length": 1}) for i in range(10)
    ]

    mock_blob.download_blob = mock_downloader
    mock_downloader.return_value.readall.return_value = b""

    azure_storage_client.download_blobs(blob_path=data_path, local_path=test_path, threads=3)

    file_count = len(os.listdir(test_path))

    assert file_count == 10
