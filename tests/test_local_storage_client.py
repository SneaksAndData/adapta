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

import pathlib
import uuid

import pandas as pd

from adapta.storage.blob.local_storage_client import LocalStorageClient
from adapta.storage.models.format import DictJsonSerializationFormat, DataFrameParquetSerializationFormat
from adapta.storage.models.local import LocalPath


def test_get_blob_uri():
    local_path = "/tmp/table"
    local_storage = LocalStorageClient.for_storage_path(f"file://{local_path}")

    assert local_storage.get_blob_uri(LocalPath(local_path)) == local_path


def test_blob_exists():
    local_path = str(pathlib.Path(__file__))
    local_storage = LocalStorageClient.for_storage_path(f"file://{local_path}")

    assert local_storage.blob_exists(LocalPath(local_path))


def test_save_data_as_blob():
    local_path = f"/tmp/{uuid.uuid4()}"
    local_storage = LocalStorageClient.for_storage_path(f"file://{local_path}")

    local_storage.save_data_as_blob(
        data={"key": "value"},
        blob_path=LocalPath(local_path),
        serialization_format=DictJsonSerializationFormat,
    )

    with open(local_path, "rb") as test_data:
        assert DictJsonSerializationFormat().deserialize(test_data.read()) == {"key": "value"}


def test_list_blobs():
    local_path = str(pathlib.Path(__file__).parent)
    local_storage = LocalStorageClient.for_storage_path(f"file://{local_path}")

    assert len(list(local_storage.list_blobs(LocalPath(local_path)))) > 0


def test_read_blobs():
    test_base = uuid.uuid4()
    local_storage = LocalStorageClient.for_storage_path(f"file:///tmp")

    local_storage.save_data_as_blob(
        data={"key1": "value1"},
        blob_path=LocalPath(f"/tmp/{test_base}/dirs/1"),
        serialization_format=DictJsonSerializationFormat,
    )

    local_storage.save_data_as_blob(
        data={"key2": "value2"},
        blob_path=LocalPath(f"/tmp/{test_base}/dirs/2"),
        serialization_format=DictJsonSerializationFormat,
    )

    dataframe = pd.DataFrame({"key3": ["value3"], "key4": ["value4"]})
    local_storage.save_data_as_blob(
        data=dataframe,
        blob_path=LocalPath(f"/tmp/{test_base}/files/3_4.parquet"),
        serialization_format=DataFrameParquetSerializationFormat,
    )

    files = list(
        local_storage.read_blobs(
            blob_path=LocalPath(f"/tmp/{test_base}/dirs/"), serialization_format=DictJsonSerializationFormat
        )
    )

    assert len(files) == 2

    parquet_from_file = pd.concat(
        local_storage.read_blobs(
            blob_path=LocalPath(f"/tmp/{test_base}/files/3_4.parquet"),
            serialization_format=DataFrameParquetSerializationFormat,
        )
    )

    assert dataframe.equals(parquet_from_file)

    parquet_from_dir = pd.concat(
        local_storage.read_blobs(
            blob_path=LocalPath(f"/tmp/{test_base}/files/"), serialization_format=DataFrameParquetSerializationFormat
        )
    )

    assert dataframe.equals(parquet_from_dir)
