import pathlib

import pandas
import pytest

from proteus.security.clients import LocalClient
from proteus.storage.models.local import LocalPath
from proteus.storage.delta_lake import load
from pyarrow.dataset import field as pyarrow_field


@pytest.fixture
def get_client_and_path():
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table"

    client = LocalClient()
    data_path = LocalPath.from_hdfs_path(f'file:///{test_data_path}')

    return client, data_path


def test_delta_load(get_client_and_path):
    client, data_path = get_client_and_path
    table: pandas.DataFrame = load(client, data_path)

    assert len(table) == 17


# important note: batch loading doesn't seem to kick off beyond specific row count. So for our test table we always get row-per-dataframe
# on real tables batching seems to work, thus we only test return data type here
def test_delta_batch_load(get_client_and_path):
    client, data_path = get_client_and_path
    table = list(load(client, data_path, batch_size=10))

    assert isinstance(table[0], pandas.DataFrame)


def test_delta_filter(get_client_and_path):
    client, data_path = get_client_and_path
    table = load(client, data_path, row_filter=(pyarrow_field('A') == "b"))

    assert len(table) == 0


def test_column_project(get_client_and_path):
    client, data_path = get_client_and_path
    table = load(client, data_path, columns=["B"])

    assert len(table.columns.to_list()) == 1
