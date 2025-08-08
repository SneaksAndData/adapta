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
import pytest

from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.aws import S3Path
from unittest.mock import patch


def test_from_hdfs_path():
    path = S3Path.from_hdfs_path("s3a://bucket/nested/key")
    assert path.bucket == "bucket"
    assert path.path == "nested/key"


def test_to_hdfs_path():
    path = S3Path("bucket", "nested/key").to_hdfs_path()
    assert path == "s3a://bucket/nested/key"


@pytest.mark.parametrize(
    "path_a, path_b, expected_result",
    [
        (
            S3Path("bucket", "nested/folder"),
            S3Path("bucket", "other/nested/key"),
            S3Path("bucket", "nested/folder/other/nested/key"),
        ),
        (S3Path("bucket", "folder"), S3Path("bucket", "key"), S3Path("bucket", "folder/key")),
    ],
)
def test_add(path_a: S3Path, path_b: S3Path, expected_result: S3Path):
    assert (path_a + path_b).to_hdfs_path() == expected_result.to_hdfs_path()


@patch("adapta.storage.blob.s3_storage_client.AwsClient")
def test_for_storage_path(mock_aws_client):
    path = "s3a://bucket/path/to/my/table"
    s3_storage_client = S3StorageClient.for_storage_path(path)
    mock_aws_client.assert_called_once()
    assert isinstance(s3_storage_client, S3StorageClient)
    assert s3_storage_client._base_client == mock_aws_client.return_value
