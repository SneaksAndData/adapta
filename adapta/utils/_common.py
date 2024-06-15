"""Common utility functions. All of these are imported into __init__.py"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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

import contextlib
import math
import os
import sys

import time
from collections import namedtuple
from functools import partial
from typing import List, Optional, Dict, Any, Tuple, Union

from pandas import DataFrame, Series, to_numeric
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry

try:
    import resource
except (ImportError, ModuleNotFoundError):
    pass


def doze(seconds: int, doze_period_ms: int = 100) -> int:
    """Sleeps for time given in seconds.

    Note for Windows users: doze_period_ms less than 15 doesn't work correctly.

    Args:
        seconds: Seconds to doze for
        doze_period_ms: Milliseconds per doze cycle

    Returns: Time elapsed in nanoseconds

    """
    loops = int(seconds * 1000 / doze_period_ms)
    start = time.monotonic_ns()
    for _ in range(loops):
        time.sleep(doze_period_ms / 1000)

    return time.monotonic_ns() - start


def session_with_retries(
    method_list: Tuple[str, ...] = ("HEAD", "GET", "OPTIONS", "TRACE"),
    request_timeout: Optional[float] = 300,
    status_list: Tuple[int, ...] = (400, 429, 500, 502, 503, 504),
    retry_count: int = 4,
):
    """
     Provisions http session manager with retries.
    :return:
    """
    retry_strategy = Retry(
        total=retry_count,
        status_forcelist=status_list,
        allowed_methods=method_list,
        backoff_factor=1,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    http = requests.Session()
    http.mount("https://", adapter)
    http.mount("http://", adapter)
    http.request = partial(http.request, timeout=request_timeout)
    http.send = partial(http.send, timeout=request_timeout)

    return http


def convert_datadog_tags(tag_dict: Optional[Dict[str, str]]) -> Optional[List[str]]:
    """
     Converts tags dictionary to Datadog tag format.

    :param tag_dict: Dictionary of tags.
    :return: A list of tag_key:tag_value
    """
    if not tag_dict:
        return None
    return [f"{k}:{v}" for k, v in tag_dict.items()]


@contextlib.contextmanager
def operation_time():
    """
      Returns execution time for the context block.
    :return: A tuple of (method_execution_time_ns, method_result)
    """
    result = namedtuple("OperationDuration", ["start", "end", "elapsed"])
    result.start = time.monotonic_ns()
    result.end = 0
    result.elapsed = 0
    yield result
    result.end = time.monotonic_ns()
    result.elapsed = result.end - result.start


def chunk_list(value: List[Any], num_chunks: int) -> List[List[Any]]:
    """
     Chunks the provided list into at most the specified number of chunks. This method is thread-safe.

    :param value: A list to chunk.
    :param num_chunks: Number of chunks to generate.
    :return: A list that has num_chunks lists in it. Total length equals length of the original list.
    """
    if num_chunks == 0:
        raise ValueError("Number of chunks must be greater than zero")
    if len(value) == 0:
        return []
    chunk_size = math.ceil(len(value) / num_chunks)
    return [value[el_pos : el_pos + chunk_size] for el_pos in range(0, len(value), chunk_size)]


@contextlib.contextmanager
def memory_limit(*, memory_limit_percentage: Optional[float] = None, memory_limit_bytes: Optional[int] = None):
    """
    Context manager to limit the amount of memory used by a process.
    On context exit, the memory limit is reset to the total memory available.

    :param memory_limit_percentage: Percentage of total memory to limit usage to.
    :param memory_limit_bytes: Number of bytes to limit usage to.

    Raises:
      ValueError: If neither memory_limit_percentage or memory_limit_bytes is specified.
    """
    if sys.platform == "win32":
        yield None
    else:
        total_mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        try:
            if memory_limit_percentage and not memory_limit_bytes:
                limit = int(total_mem_bytes * memory_limit_percentage)
                resource.setrlimit(
                    resource.RLIMIT_AS,
                    (limit, total_mem_bytes),
                )
                yield limit

            elif memory_limit_bytes and not memory_limit_percentage:
                resource.setrlimit(
                    resource.RLIMIT_AS,
                    (memory_limit_bytes, total_mem_bytes),
                )

                yield memory_limit_bytes
            else:
                raise ValueError("Specify either memory_limit_percentage or memory_limit_bytes")
        finally:
            resource.setrlimit(resource.RLIMIT_AS, (total_mem_bytes, total_mem_bytes))


def map_column_names(
    dataframe: DataFrame,
    column_map: Dict[str, str],
    default_values: Optional[Dict[str, Union[str, int, float]]] = None,
    drop_missing: bool = True,
) -> DataFrame:
    """
    Maps a dataframe from one nomenclature to another. Original dataframe is not mutated.

    :param dataframe: Dataframe to be mapped.
    :param column_map: A dictionary mapping old column names to new.
    :param default_values: If a column is not present in the dataframe
    a default value mapping can be given, by mapping a column name it a value.
    :param drop_missing: A boolean value to control if columns should be
    dropped if the columns are present in the dataframe but not the column_map.
    """
    default_values = default_values or {}
    # Only columns in the map are mapped
    kept_columns = list(set(column_map.keys()) & set(dataframe.columns)) if drop_missing else dataframe.columns
    dataframe = dataframe[kept_columns].rename(columns=column_map, errors="ignore")
    # Only use default values for columns not present in the dataframe
    default_values = {k: v for (k, v) in default_values.items() if k not in dataframe.columns}
    dataframe[list(default_values.keys())] = list(default_values.values())
    return dataframe


def downcast_dataframe(dataframe: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
    """
    Downcasts a Pandas dataframe to the smallest possible data type for each column. Only interger and float
    columns are downcasted. Other columns are left as is.

    :param dataframe: A Pandas dataframe.
    :param columns: A list of columns to downcast. If None, all columns are downcasted.

    :return: The downcasted Pandas dataframe.
    """

    columns = list(dataframe.columns) if columns is None else columns

    def get_downcast_type(column: Series) -> Optional[str]:
        """
        Returns the downcast type for a Pandas column.

        :param column: A Pandas series.
        :return: The downcast type for the column.
        """
        if column.dtype.kind == "f":
            return "float"
        if column.dtype.kind == "i":
            return "integer"
        if column.dtype.kind == "u":
            return "unsigned"
        raise ValueError(f"Unsupported dtype: {column.dtype}")

    def downcast_supported(column: Series) -> bool:
        """
        Checks if a Pandas column can be downcasted.

        :param column: A Pandas series.
        :return: True if the column can be downcasted, False otherwise.
        """
        return column.dtype.kind in ["f", "i", "u"]

    return dataframe.assign(
        **{
            column: lambda x, c=column: to_numeric(x[c], downcast=get_downcast_type(x[c]))
            if downcast_supported(x[c])
            else x[c]
            for column in dataframe.columns
            if column in columns
        }
    )
