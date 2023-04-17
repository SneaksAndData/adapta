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
import os
import sys

import time
from typing import List, Any, Dict, Optional

import pytest

from adapta.utils import doze, operation_time, chunk_list, memory_limit
from adapta.utils.concurrent_task_runner import Executable, ConcurrentTaskRunner


@pytest.mark.parametrize("sleep_period,doze_interval", [(1, 50), (2, 10)])
def test_doze(sleep_period: int, doze_interval: int):
    time_passed = doze(sleep_period, doze_interval) // 1e9

    assert int(time_passed) == sleep_period


def test_operation_time():
    def custom_method():
        time.sleep(5)
        return {"exit_code": 0}

    with operation_time() as ot:
        result = custom_method()

    assert (ot.elapsed // 1e9, result) == (5, {"exit_code": 0})


@pytest.mark.parametrize(
    "list_to_chunk,num_chunks,expected_list",
    [
        (list(range(10)), 3, [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9]]),
        (list(range(10)), 2, [[0, 1, 2, 3, 4], [5, 6, 7, 8, 9]]),
    ],
)
def test_chunk_list(list_to_chunk: List[Any], num_chunks: int, expected_list):
    assert chunk_list(list_to_chunk, num_chunks) == expected_list


def mock_func(a: float, b: str, c: bool) -> Dict:
    time.sleep(a)
    return {"a": a, "b": b, "c": c}


@pytest.mark.parametrize(
    "func_list,num_threads,use_processes,expectations,expected_wait",
    [
        (
            # Run 3 functions in 3 threads
            # Each function sleeps for `a` seconds before returning
            # Since we do lazy result fetch, we should expect to wait LESS than max(a0,.. aN), because all tasks effectively start at the same time
            # however since time.sleep effectively blocks the main thread if using ThreadPoolExecutor, subsequent submissions will delay each other
            # thus we should expect at most 0.5s + small time to get results of each future.
            [
                Executable[Dict](mock_func, [0.1, "test", True], "case1"),
                Executable[Dict](mock_func, [0.3, "test1", True], "case2"),
                Executable[Dict](mock_func, [0.5, "test2", False], "case3"),
            ],
            3,
            False,
            {
                "case1": {"a": 0.1, "b": "test", "c": True},
                "case2": {"a": 0.3, "b": "test1", "c": True},
                "case3": {"a": 0.5, "b": "test2", "c": False},
            },
            0.55,
        ),
        # Runs 1 thread for each function
        # Expected to see 1s + 2s + 3s + result process time ~ slightly above 6s
        (
            [
                Executable[Dict](mock_func, [1, "test", True], "case1"),
                Executable[Dict](mock_func, [2, "test1", True], "case2"),
                Executable[Dict](mock_func, [3, "test2", False], "case3"),
            ],
            1,
            False,
            {
                "case1": {"a": 1, "b": "test", "c": True},
                "case2": {"a": 2, "b": "test1", "c": True},
                "case3": {"a": 3, "b": "test2", "c": False},
            },
            6.1,
        ),
        # Runs 3 processes for 3 functions
        # Same as the second test case, but now we use ProcessPoolExecutor, so we should expect 3s + process start time overhead
        (
            [
                Executable[Dict](mock_func, [1, "test", True], "case1"),
                Executable[Dict](mock_func, [2, "test1", True], "case2"),
                Executable[Dict](mock_func, [3, "test2", False], "case3"),
            ],
            3,
            True,
            {
                "case1": {"a": 1, "b": "test", "c": True},
                "case2": {"a": 2, "b": "test1", "c": True},
                "case3": {"a": 3, "b": "test2", "c": False},
            },
            4,
        ),
    ],
)
def test_concurrent_task_runner(
    func_list: List[Executable[Dict]],
    num_threads: int,
    use_processes: bool,
    expectations: Dict[str, Dict],
    expected_wait: float,
):
    start = time.monotonic_ns()
    runner = ConcurrentTaskRunner(func_list, num_threads, use_processes)
    tasks = runner.lazy()
    results = {}
    for task_name, task_future in tasks.items():
        results[task_name] = task_future.result()
    total_wait = (time.monotonic_ns() - start) / 1e9

    assert results == expectations and total_wait < expected_wait


@pytest.mark.skipif(sys.platform == "win32", reason="Functionality not supported on Windows")
@pytest.mark.parametrize(
    "limit_bytes,limit_percentage,num_iterations,expected_limit",
    [
        (512, None, 1024, 512),
        (None, 0.8, 1024 * 1024, int(0.8 * os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")))
        if sys.platform != "win32"
        else None,
    ],
)
def test_memory_limit_enough_memory(
    limit_bytes: Optional[int], limit_percentage: Optional[float], num_iterations: int, expected_limit: int
):
    """
    This unit test method verifies that the function `memory_limit` correctly enforces the given memory limit.
    The test is skipped if the platform is Windows since the functionality is not supported there.

    Test 1:
    - limit_bytes: 512
    - limit_percentage: None
    - num_iterations: 1024
    - expected_limit: 512

    This test checks that the function correctly enforces a memory limit of 512 bytes when given a byte limit.

    Test 2:
    - limit_bytes: None
    - limit_percentage: 0.8
    - num_iterations: 1024*1024
    - expected_limit: int(0.8 * os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES"))

    This test checks that the function correctly enforces a memory limit of 80% of the total memory when given a percentage limit.
    """
    test_str = "a"
    with memory_limit(memory_limit_bytes=limit_bytes, memory_limit_percentage=limit_percentage) as enforced_limit:
        test_str *= num_iterations
    assert enforced_limit == expected_limit


@pytest.mark.skipif(sys.platform == "win32", reason="Functionality not supported on Windows")
@pytest.mark.parametrize(
    "limit_bytes,limit_percentage,num_iterations",
    [
        (512, None, 1024 * 1024),
        (None, 1e-9, 1024 * 1024),
    ],
)
def test_memory_limit_error(limit_bytes: Optional[int], limit_percentage: Optional[float], num_iterations: int):
    """
     This unit test method is testing the `memory_limit` function for correct handling of MemoryError exceptions. The test is skipped on Windows as the functionality is not supported on this platform.

     Test case 1:

    - `limit_bytes` is set to 512 bytes
    - `limit_percentage` is set to None
    - `num_iterations` is set to 1024 * 1024

    Test case 2:

    - `limit_bytes` is set to None
    - `limit_percentage` is set to 1e-9
    - `num_iterations` is set to 1024 * 1024

    In both test cases, the test expects a MemoryError exception to be raised when `test_str` is multiplied by `num_iterations`.
    """
    test_str = "a"
    with pytest.raises(MemoryError):
        with memory_limit(memory_limit_bytes=limit_bytes, memory_limit_percentage=limit_percentage):
            test_str *= num_iterations
