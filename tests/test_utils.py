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
from logging import StreamHandler
from typing import List, Any, Dict, Optional

import numpy as np
import pandas
import pandas as pd
import pytest

from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel
from adapta.utils import (
    doze,
    operation_time,
    chunk_list,
    memory_limit,
    map_column_names,
    run_time_metrics,
    downcast_dataframe,
)
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
                Executable[Dict](func=mock_func, args=[0.1, "test", True], alias="case1"),
                Executable[Dict](func=mock_func, args=[0.3, "test1", True], alias="case2"),
                Executable[Dict](func=mock_func, args=[0.5, "test2", False], alias="case3"),
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
                Executable[Dict](func=mock_func, args=[1, "test", True], alias="case1"),
                Executable[Dict](func=mock_func, args=[2, "test1", True], alias="case2"),
                Executable[Dict](func=mock_func, args=[3, "test2", False], alias="case3"),
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
                Executable[Dict](func=mock_func, args=[1, "test", True], alias="case1"),
                Executable[Dict](func=mock_func, args=[2, "test1", True], alias="case2"),
                Executable[Dict](func=mock_func, args=[3, "test2", False], alias="case3"),
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
        # Runs 3 processes for 3 functions
        # Same as the third test case, but using kwargs instead of args. Exact same result expected
        (
            [
                Executable[Dict](func=mock_func, kwargs={"a": 1, "b": "test", "c": True}, alias="case1"),
                Executable[Dict](func=mock_func, kwargs={"a": 2, "b": "test1", "c": True}, alias="case2"),
                Executable[Dict](func=mock_func, kwargs={"a": 3, "b": "test2", "c": False}, alias="case3"),
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
        (512, None, 1024 * 1024 * 1024),
        (None, 1e-9, 1024 * 1024 * 1024),
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


@pytest.mark.parametrize("drop_missing", [True, False])
def test_data_adapter(drop_missing: bool):
    """
    Testing that generic mapping of columns work.
    Test checks if column names are mapped, default columns
    don't overwrite existing columns and are added if a
    column is missing.

    :param drop_missing: If columns missing from the mapping
    dictionary should be dropped.
    """
    data = pandas.DataFrame(data={"A": [1, 2, 3], "B": [4, 5, 6]})

    column_map = {"A": "C"}

    default_values = {"C": 9, "D": 7}

    result = map_column_names(data, column_map, default_values, drop_missing=drop_missing)

    assert len(result) == 3
    assert len(result.columns) == 2 if drop_missing else 3

    assert "A" not in result.columns
    assert ("B" not in result.columns) if drop_missing else ("B" in result.columns)
    assert "C" in result.columns
    assert "D" in result.columns

    assert (result["C"] != 7).all()
    assert (result["C"] != 9).all()
    assert (result["D"] == 7).all()


@pytest.mark.parametrize("reporting_level", [LogLevel.DEBUG, LogLevel.INFO])
@pytest.mark.parametrize("loglevel", [LogLevel.DEBUG, LogLevel.INFO])
@pytest.mark.parametrize("tag_func_name", [True, False])
def test_runtime_decorator(caplog, reporting_level, loglevel, tag_func_name):
    """
    Test that run_time_metrics_decorator reports correct information for every run of the algorithm.

    Firstly tests that wrapped method executes even when no logger is passed
    Secondly tests that wrapped method sends logs when logger is passed.

    :param caplog: pytest fixture for testing logging.
    :param reporting_level: Reporting level defining at what level decorator sends logs.
    :param loglevel: Loglevel that is tested.
    """
    logger = SemanticLogger().add_log_source(
        log_source_name="decorator_test",
        min_log_level=loglevel,
        log_handlers=[StreamHandler(sys.stdout)],
        is_default=True,
    )

    class DummyMetricProvider:
        def gauge(self, metric_name, metric_value, tags):
            """Dummy provider to assert passed values"""
            assert metric_name == run_type
            assert type(metric_value) == float
            assert not tag_func_name or tags["function_name"] == "test_function"

    metrics_provider = DummyMetricProvider()
    run_type = "test_execution"
    print_from_func = "from_function_call"

    @run_time_metrics(metric_name=run_type, tag_function_name=True, log_level=reporting_level)
    def test_function(logger, **_kwargs):
        logger.info(print_from_func)
        return True

    test_function(logger=logger, metrics_provider=metrics_provider)
    if loglevel == LogLevel.DEBUG:
        assert "test_function" in caplog.text and run_type in caplog.text
        assert "finished in" in caplog.text and "s seconds" in caplog.text
    elif loglevel == LogLevel.INFO:
        assert "DEBUG" not in caplog.text
    assert print_from_func in caplog.text


def test_missing_decorator_error():
    """Assert that readable error is raised when decorator (logger, metric provider) attributes are missing"""

    @run_time_metrics(metric_name="test_execution")
    def test_function(**_kwargs):
        return

    with pytest.raises(AttributeError):
        test_function()


@pytest.mark.parametrize(
    "dataframe, expected_types, column_filter",
    [
        (
            pandas.DataFrame(data={"A": [1, 2, 3], "B": pandas.Series([4, None, 6], dtype=pd.Int64Dtype())}),
            {"A": "int8", "B": "Int8"},
            None,
        ),
        (pandas.DataFrame(data={"A": [1, 2, 3], "B": [4, 5, 6]}), {"A": "int8", "B": "int64"}, ["A"]),
        (pandas.DataFrame(data={"A": [1, 2, 3], "B": [4, 5, 6]}), {"A": "int64", "B": "int64"}, []),
        (pandas.DataFrame(data={"A": [1000, 2, 3], "B": [4, 5, 6]}), {"A": "int16", "B": "int8"}, None),
        (pandas.DataFrame(data={"A": [10000000, 2, 3], "B": [4, 5, 6]}), {"A": "int32", "B": "int8"}, None),
        (pandas.DataFrame(data={"A": [100000000000, 2, 3], "B": [4, 5, 6]}), {"A": "int64", "B": "int8"}, None),
        (
            pandas.DataFrame(data={"A": [1.0, 2.0, 3.0], "B": [4.0, np.nan, 6.0]}),
            {"A": "float32", "B": "float32"},
            None,
        ),
        (pandas.DataFrame(data={"A": [1.0, 2.0, 3.0], "B": ["a", "b", "c"]}), {"A": "float32", "B": "object"}, None),
        (pandas.DataFrame(data={"A": [1, 0, 1]}), {"A": "int8"}, None),
        (pandas.DataFrame(data={"A": pandas.Series([4, 2, 6], dtype="uint32")}), {"A": "uint8"}, None),
    ],
)
def test_downcast_dataframe(dataframe, expected_types, column_filter):
    """
    Test that downcast_dataframe works as expected.

    :param dataframe: Dataframe to downcast.
    :param expected_types: Expected types of columns after downcast.
    :param column_filter: Columns to downcast.
    """
    result = downcast_dataframe(dataframe, columns=column_filter)
    for column in expected_types:
        assert result[column].dtype == expected_types[column]
