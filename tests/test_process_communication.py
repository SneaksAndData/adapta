from typing import Optional, Iterable, Union, Dict

import pytest

from proteus.process_communication import DataSocket
from proteus.storage.models.azure import AdlsGen2Path
from proteus.storage.models.base import DataPath
from proteus.storage.models.local import LocalPath
from proteus.storage.models.format import SerializationFormat, DataFrameCsvSerializationFormat, \
    DictJsonSerializationFormat, DataFrameJsonSerializationFormat


@pytest.mark.parametrize(
    'alias,data_path,data_format,expected_ser',
    [
        ("test", "file://some-folder/here", "jpg", "test|file://some-folder/here|jpg"),
        ("", "file://some-folder/here", "jpg", None),
        (None, "file://some-folder/here", "", None),
    ]
)
def test_data_socket_serialize(alias: str, data_path: str, data_format: str, expected_ser: Optional[str]):
    try:
        test_socket = DataSocket(alias, data_path, data_format)
        assert test_socket.serialize() == expected_ser
    except AssertionError:
        if expected_ser:
            raise


@pytest.mark.parametrize(
    'value,expected_deser',
    [
        (
                {
                    "alias": "test",
                    "data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "data_format": "delta"
                },
                DataSocket(
                    "test",
                    "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "delta"
                )
        ),
        (
                '{"alias": "test","data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table","data_format": "delta"}',
                DataSocket(
                    "test",
                    "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "delta"
                )
        )
    ]
)
def test_socket_from_json(value: Union[str, Dict], expected_deser: Optional[DataSocket]):
    test_socket = DataSocket.from_json(value) if type(value) is str else DataSocket.from_dict(value)

    assert test_socket == expected_deser


@pytest.mark.parametrize(
    'value,expected_deser',
    [
        ("test|file://some-folder/here|jpg", DataSocket("test", "file://some-folder/here", "jpg")),
        ("test|file://some-folder/here|", None),
    ]
)
def test_data_socket_deserialize(value: str, expected_deser: Optional[DataSocket]):
    try:
        test_socket = DataSocket.deserialize(value)
        assert test_socket == expected_deser
    except AssertionError:
        if expected_deser:
            raise


@pytest.mark.parametrize(
    'alias,data_path,data_format,expected_path',
    [
        ("test", "file://some-folder/file", "text", LocalPath(path='some-folder/file')),
        ("test", "abfss://container@account.dfs.core.windows.net/some-folder/file", "text",
         AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/some-folder/file")),
        ("test", "api://some-folder/file", "json", None),
    ]
)
def test_path_parse(alias: str, data_path: str, data_format: str, expected_path: Optional[DataPath]):
    test_socket = DataSocket(alias, data_path, data_format)

    assert test_socket.parse_data_path() == expected_path
