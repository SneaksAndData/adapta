from typing import Optional, Iterable

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
        ("test", "abfss://container@account.dfs.core.windows.net/some-folder/file", "text", AdlsGen2Path.from_hdfs_path("abfss://container@account.dfs.core.windows.net/some-folder/file")),
        ("test", "api://some-folder/file", "json", None),
    ]
)
def test_path_parse(alias: str, data_path: str, data_format: str, expected_path: Optional[DataPath]):
    test_socket = DataSocket(alias, data_path, data_format)

    assert test_socket.parse_data_path() == expected_path


@pytest.mark.parametrize(
    'alias,data_path,data_format,expected_formats',
    [
        ("test", "file://some-folder/file", "csv", [DataFrameCsvSerializationFormat]),
        ("test", "file://some-folder/file", "json", [DictJsonSerializationFormat, DataFrameJsonSerializationFormat]),
        ("test", "file://some-folder/file", "webp", []),
    ]
)
def test_format_parse(alias: str, data_path: str, data_format: str, expected_formats: Iterable[SerializationFormat]):
    test_socket = DataSocket(alias, data_path, data_format)

    assert set(test_socket.parse_serialization_format()) == set(expected_formats)
