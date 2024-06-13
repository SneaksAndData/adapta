#  Copyright (c) 2024. ECCO Sneaks & Data
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

from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.aws import S3Path
from unittest.mock import patch, MagicMock

def test_from_hdfs_path():
    path = S3Path.from_hdfs_path("s3a://bucket/nested/key")
    assert path.bucket == "bucket"
    assert path.path == "nested/key"

def test_to_hdfs_path():
    path = S3Path("bucket", "nested/key").to_hdfs_path()
    assert path == "s3a://bucket/nested/key"

@patch("adapta.storage.blob.s3_storage_client.AwsClient")
def test_for_storage_path(mock_aws_client):
    path = "s3a://bucket/path/to/my/table"
    s3_storage_client = S3StorageClient.for_storage_path(path)
    mock_aws_client.assert_called_once()
    assert isinstance(s3_storage_client, S3StorageClient)
    assert s3_storage_client._base_client == mock_aws_client.return_value

@patch("boto3.resource")
@patch("boto3.Session")
@patch("adapta.security.clients.AwsClient")
def test_get_blob_uri(mock_client: MagicMock, mock_session: MagicMock, mock_s3_resource: MagicMock):
    mock_client_instance: AwsClient = mock_client.return_value
    s3_path = S3Path.from_hdfs_path("s3a://bucket/path/to/my/table")
    mock_client_instance.initialize_session.return_value = {
        "AWS_ACCESS_KEY_ID": "test",
        "AWS_SECRET_ACCESS_KEY": "test",
    }

    s3_storage_client = S3StorageClient(base_client=mock_client_instance)
    uri = s3_storage_client.get_blob_uri(s3_path)
    assert uri == "s3://bucket/path/to/my/table"
