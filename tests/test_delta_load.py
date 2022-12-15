import pathlib
from unittest.mock import patch, MagicMock, ANY, call

import pandas
import pytest

from proteus.security.clients import LocalClient
from proteus.storage.models.local import LocalPath
from proteus.storage.delta_lake import load, load_cached, get_cache_key
from proteus.storage.cache import KeyValueCache
from proteus.storage.models.format import DataFrameParquetSerializationFormat

from pyarrow.dataset import field as pyarrow_field


@pytest.fixture
def get_client_and_path():
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table"

    client = LocalClient()
    data_path = LocalPath.from_hdfs_path(f'file://{test_data_path}')

    return client, data_path


@pytest.fixture
def get_client_and_path_partitioned():
    test_data_path = f"{pathlib.Path(__file__).parent.resolve()}/delta_table_with_partitions"

    client = LocalClient()
    data_path = LocalPath.from_hdfs_path(f'file://{test_data_path}')

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


def test_delta_load_with_partitions(get_client_and_path_partitioned):
    client, data_path = get_client_and_path_partitioned
    table = load(client, data_path, partition_filter_expressions=[("colP", "=", "yes")])

    assert table['colA'].to_list() == [1, 3]


@patch('proteus.storage.cache.KeyValueCache')
def test_delta_load_cached(mock_cache: MagicMock, get_client_and_path):
    client, data_path = get_client_and_path

    cache: KeyValueCache = mock_cache.return_value

    cache.exists.return_value = True
    cache.get.return_value = 10
    cache.multi_get.return_value = [
        DataFrameParquetSerializationFormat().serialize(pandas.DataFrame([{'a': 1, 'b': 2}]))]

    cache_key = get_cache_key(client, data_path, batch_size=1)

    _ = load_cached(client, data_path, cache=cache, batch_size=1)

    cache.exists.assert_called_with(f"{cache_key}_size")
    cache.multi_get.assert_called_with(
        [f"{cache_key}_{batch_number}" for batch_number in range(0, 10)])


@patch('proteus.storage.cache.KeyValueCache')
def test_delta_populate_cache(mock_cache: MagicMock, get_client_and_path):
    client, data_path = get_client_and_path

    cache: KeyValueCache = mock_cache.return_value

    cache.exists.return_value = False
    cache.set.return_value = None

    cache_key = get_cache_key(client, data_path, batch_size=1)

    _ = load_cached(client, data_path, cache=cache, batch_size=1)

    cache.exists.assert_called_once_with(cache_key, 'completed')

    set_calls = [call(key=cache_key, attribute=str(batch_number), value=ANY) for batch_number in range(17)]
    set_calls.append(call(key=cache_key, attribute='completed', value=ANY))

    cache.include.assert_has_calls(set_calls)
