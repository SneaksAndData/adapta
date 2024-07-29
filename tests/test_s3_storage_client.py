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


def test_valid_s3_datapath():
    malformed_s3_datapaths = [
        lambda: S3Path(bucket="bucket", path=""),
        lambda: S3Path(bucket="bucket", path="path"),
        lambda: S3Path(bucket="bucket", path="path/"),
        lambda: S3Path(bucket="bucket", path="path/path_segment"),
        lambda: S3Path(bucket="bucket", path="path/path_segment/path_segment"),
    ]

    for new_s3_data_path in malformed_s3_datapaths:
        new_s3_data_path()


def test_invalid_s3_datapath():
    malformed_s3_datapaths = [
        lambda: S3Path(bucket="/bucket/", path="path"),
        lambda: S3Path(bucket="/bucket", path="path"),
        lambda: S3Path(bucket="bucket", path="/path"),
        lambda: S3Path(bucket="bucket", path="path//path_segment"),
        lambda: S3Path(bucket="bucket", path="path/path_segment//path_segment"),
    ]

    for new_s3_data_path in malformed_s3_datapaths:
        with pytest.raises(ValueError, match=r"Invalid S3Path provided, must comply with : .*"):
            new_s3_data_path()


def test_base_uri():
    path = S3Path(bucket="bucket", path="nested/key")
    assert path.base_uri() == "https://bucket.s3.amazonaws.com"


def test_from_hdfs_path():
    path = S3Path.from_hdfs_path("s3a://bucket/nested/key")
    assert path.bucket == "bucket"
    assert path.path == "nested/key"


def test_to_uri():
    bucket_name = "bucket"
    path = "nested/key"
    path_instance = S3Path(bucket=bucket_name, path=path)
    assert path_instance.to_uri() == f"s3a://{bucket_name}/{path}"


def test_to_delta_rs_path():
    bucket_name = "bucket"
    path = "nested/key"
    path_instance = S3Path(bucket=bucket_name, path=path)
    assert path_instance.to_delta_rs_path() == f"s3a://bucket/nested/key"


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
