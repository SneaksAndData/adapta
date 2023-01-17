import time
from typing import List, Any, Callable, Dict, Tuple

import pytest
from proteus.utils import doze, operation_time, chunk_list, parallelise


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


def mock_func(a: float, b: str, c: bool) -> Dict:
    time.sleep(a)
    return {
        'a': a,
        'b': b,
        'c': c
    }


@pytest.mark.parametrize('func_list,num_threads,use_processes,expectations,expected_wait', [
    (
            # Run 3 functions in 3 threads
            # Each function sleeps for `a` seconds before returning
            # Since we do lazy result fetch, we should expect to wait LESS than max(a0,.. aN), because all tasks effectively start at the same time
            # however since time.sleep effectively blocks the main thread if using ThreadPoolExecutor, subsequent submissions will delay each other
            # thus we should expect at most 0.5s + small time to get results of each future.
            [
                (mock_func, [0.1, 'test', True], 'case1'),
                (mock_func, [0.3, 'test1', True], 'case2'),
                (mock_func, [0.5, 'test2', False], 'case3')
            ],
            3,
            False,
            {
                'case1': {
                    'a': 0.1,
                    'b': 'test',
                    'c': True
                },
                'case2': {
                    'a': 0.3,
                    'b': 'test1',
                    'c': True
                },
                'case3': {
                    'a': 0.5,
                    'b': 'test2',
                    'c': False
                }
            },
            0.55
    ),
    # Runs 1 thread for each function
    # Expected to see 1s + 2s + 3s + result process time ~ slightly above 6s
    (
            [
                (mock_func, [1, 'test', True], 'case1'),
                (mock_func, [2, 'test1', True], 'case2'),
                (mock_func, [3, 'test2', False], 'case3')
            ],
            1,
            False,
            {
                'case1': {
                    'a': 1,
                    'b': 'test',
                    'c': True
                },
                'case2': {
                    'a': 2,
                    'b': 'test1',
                    'c': True
                },
                'case3': {
                    'a': 3,
                    'b': 'test2',
                    'c': False
                }
            },
            6.1
    ),
    # Runs 3 processes for 3 functions
    # Same as the second test case, but now we use ProcessPoolExecutor, so we should expect 3s + process start time overhead
    (
            [
                (mock_func, [1, 'test', True], 'case1'),
                (mock_func, [2, 'test1', True], 'case2'),
                (mock_func, [3, 'test2', False], 'case3')
            ],
            3,
            True,
            {
                'case1': {
                    'a': 1,
                    'b': 'test',
                    'c': True
                },
                'case2': {
                    'a': 2,
                    'b': 'test1',
                    'c': True
                },
                'case3': {
                    'a': 3,
                    'b': 'test2',
                    'c': False
                }
            },
            4
    )
])
def test_parallelise(
        func_list: List[Tuple[Callable[[...], Any], List[Any], str]],
        num_threads: int,
        use_processes: bool,
        expectations: Dict[str, Dict],
        expected_wait: float
):
    start = time.monotonic_ns()
    tasks = parallelise(func_list, num_threads, use_processes)
    results = {}
    for task_name, task_future in tasks.items():
        results[task_name] = task_future.result()
    total_wait = (time.monotonic_ns() - start) / 1e9

    assert results == expectations and total_wait < expected_wait
