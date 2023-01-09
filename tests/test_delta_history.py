import pathlib

import pytest

from proteus.security.clients import LocalClient
from proteus.storage.models.local import LocalPath
from proteus.storage.delta_lake import history, DeltaOperation


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

    assert len(transactions) == 2 and operations == [DeltaOperation.WRITE.value, DeltaOperation.UPDATE.value]


def test_delta_history_full(get_client_and_path):
    client, data_path = get_client_and_path
    transactions = list(history(client, data_path, limit=None))
    operations = [tran.operation.value for tran in transactions]

    assert len(transactions) == 3 and operations == [
        DeltaOperation.CREATE_TABLE_AS_SELECT.value,
        DeltaOperation.WRITE.value,
        DeltaOperation.UPDATE.value,
    ]
