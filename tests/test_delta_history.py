#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
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
from datetime import datetime, timedelta

import pandas
import pytest

from adapta.security.clients import LocalClient
from adapta.storage.delta_lake.v3 import load
from adapta.storage.models.local import LocalPath
from adapta.storage.delta_lake import history, DeltaOperation


@pytest.fixture
def get_client_and_path():
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table_with_history"

    client = LocalClient()
    data_path = LocalPath.from_hdfs_path(f"file:///{test_data_path}")

    return client, data_path


def test_delta_history(get_client_and_path):
    client, data_path = get_client_and_path
    transactions = list(history(client, data_path))
    operations = [tran.operation.value for tran in transactions]

    assert len(transactions) == 1 and operations == [DeltaOperation.UPDATE.value]


def test_delta_history_2(get_client_and_path):
    client, data_path = get_client_and_path
    transactions = list(history(client, data_path, limit=2))
    operations = [tran.operation.value for tran in transactions]

    assert len(transactions) == 2 and operations == [
        DeltaOperation.UPDATE.value,
        DeltaOperation.WRITE.value,
    ]


def test_delta_history_full(get_client_and_path):
    client, data_path = get_client_and_path
    transactions = list(history(client, data_path, limit=None))
    operations = [tran.operation.value for tran in transactions]

    assert len(transactions) == 3 and operations == [
        DeltaOperation.UPDATE.value,
        DeltaOperation.WRITE.value,
        DeltaOperation.CREATE_TABLE_AS_SELECT.value,
    ]


@pytest.mark.parametrize("timestamp", [datetime(year=1900, month=1, day=1)])
def test_delta_time_travel(get_client_and_path, timestamp):
    client, data_path = get_client_and_path
    current_table: pandas.DataFrame = load(client, data_path).to_pandas()
    first_version_table: pandas.DataFrame = load(client, data_path, timestamp=timestamp).to_pandas()
    latest_version_table: pandas.DataFrame = load(
        client, data_path, timestamp=datetime.now() + timedelta(days=10)
    ).to_pandas()

    assert current_table.equals(latest_version_table)
    assert not current_table.equals(first_version_table)
