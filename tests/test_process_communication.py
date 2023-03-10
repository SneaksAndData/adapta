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

from contextlib import nullcontext as does_not_raise, AbstractContextManager
from typing import Optional, Union, Dict

import pytest

from adapta.process_communication import DataSocket
from adapta.storage.models.azure import AdlsGen2Path
from adapta.storage.models.base import DataPath
from adapta.storage.models.local import LocalPath


@pytest.mark.parametrize(
    "alias,data_path,data_format,expectation_handler",
    [
        (
            "test",
            "file://some-folder/here",
            "jpg",
            does_not_raise("test|file://some-folder/here|jpg"),
        ),
        ("", "file://some-folder/here", "jpg", pytest.raises(AssertionError)),
        (None, "file://some-folder/here", "", pytest.raises(AssertionError)),
    ],
)
def test_data_socket_serialize(
    alias: str,
    data_path: str,
    data_format: str,
    expectation_handler: AbstractContextManager,
):
    """
      DataSocket must instantiate and `serialize()` itself correctly, given correct input.

    :param alias: Alias for the socket.
    :param data_path: Data path for the socket.
    :param data_format: Data format represented by the socket.
    :param expectation_handler: Test expectation handler.
    :return:
    """
    with expectation_handler:
        test_socket = DataSocket(alias, data_path, data_format)
        assert test_socket.serialize() == expectation_handler.enter_result


@pytest.mark.parametrize(
    "value,expectation_handler",
    [
        (
            {
                "alias": "test",
                "data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                "data_format": "delta",
            },
            does_not_raise(
                DataSocket(
                    "test",
                    "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "delta",
                )
            ),
        ),
        (
            '{"alias": "test","data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table","data_format": "delta"}',
            does_not_raise(
                DataSocket(
                    "test",
                    "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "delta",
                )
            ),
        ),
        (
            {
                "alias": "test",
                "data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                "data_format": "delta",
                "data_partitions": ["colA", "colB"],
            },
            does_not_raise(
                DataSocket(
                    "test",
                    "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "delta",
                    ["colA", "colB"],
                )
            ),
        ),
        (
            '{"alias": "test","data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table","data_format": "delta","data_partitions":["colA","colB"]}',
            does_not_raise(
                DataSocket(
                    "test",
                    "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                    "delta",
                    ["colA", "colB"],
                )
            ),
        ),
        (
            {
                "alias": "",
                "data_path": "abfss://some-data@azureaccount.dfs.core.windows.net/some_folder/some_table",
                "data_format": "delta",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_socket_from_json(
    value: Union[str, Dict], expectation_handler: AbstractContextManager
):
    """
      DataSocket must deserialize from json text and from dict correctly. Invalid values must throw an assertion error.

    :param value: Dict or json value to instantiate a DataSocket from.
    :param expectation_handler: Test expectation handler.
    :return:
    """
    with expectation_handler:
        test_socket = (
            DataSocket.from_json(value)
            if type(value) is str
            else DataSocket.from_dict(value)
        )
        assert test_socket == expectation_handler.enter_result


@pytest.mark.parametrize(
    "value,expectation_handler",
    [
        (
            "test|file://some-folder/here|jpg",
            does_not_raise(DataSocket("test", "file://some-folder/here", "jpg")),
        ),
        ("test|file://some-folder/here|", pytest.raises(AssertionError)),
    ],
)
def test_data_socket_deserialize(
    value: str, expectation_handler: AbstractContextManager
):
    """
     DataSocket must deserialize from a |-delimited string value correctly and throw an assertion error if missing any required attributes.

    :param value: A value to deserialize as a DataSocket
    :param expectation_handler: Test expectation handler.
    :return:
    """
    with expectation_handler:
        test_socket = DataSocket.deserialize(value)
        assert test_socket == expectation_handler.enter_result


@pytest.mark.parametrize(
    "alias,data_path,data_format,expected_path",
    [
        ("test", "file://some-folder/file", "text", LocalPath(path="some-folder/file")),
        (
            "test",
            "abfss://container@account.dfs.core.windows.net/some-folder/file",
            "text",
            AdlsGen2Path.from_hdfs_path(
                "abfss://container@account.dfs.core.windows.net/some-folder/file"
            ),
        ),
        ("test", "api://some-folder/file", "json", None),
    ],
)
def test_path_parse(
    alias: str, data_path: str, data_format: str, expected_path: Optional[DataPath]
):
    """
      DataSocket must parse data_path to one of the supported paths or return None if a given path format is unknown.

    :param alias: Alias for the socket.
    :param data_path: Data path for the socket.
    :param data_format: Data format represented by the socket.
    :param expected_path: Instantiated `DataPath` object, if a given format is supported.
    :return:
    """
    test_socket = DataSocket(alias, data_path, data_format)

    assert test_socket.parse_data_path() == expected_path
