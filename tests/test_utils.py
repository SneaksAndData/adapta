import time
from typing import List, Any

import pytest
from proteus.utils import doze, operation_time, chunk_list


@pytest.mark.parametrize('sleep_period,doze_interval', [(1, 50), (2, 10)])
def test_doze(sleep_period: int, doze_interval: int):
    time_passed = doze(sleep_period, doze_interval) // 1e9

    assert int(time_passed) == sleep_period


def test_operation_time():
    def custom_method():
        time.sleep(5)
        return {'exit_code': 0}

    with operation_time() as ot:
        result = custom_method()

    assert (ot.elapsed // 1e9, result) == (5, {'exit_code': 0})


@pytest.mark.parametrize('list_to_chunk,num_chunks,expected_list', [
    (list(range(10)), 3, [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]),
    (list(range(10)), 2, [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]])
])
def test_chunk_list(list_to_chunk: List[Any], num_chunks: int, expected_list):
    assert chunk_list(list_to_chunk, num_chunks) == expected_list
