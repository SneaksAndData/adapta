"""Common utility functions. All of these are imported into __init__.py"""
import concurrent.futures
import contextlib
import math
import os
import sys
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from functools import partial
from typing import List, Optional, Dict, Any, Callable, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


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


def session_with_retries(method_list: Optional[List[str]] = None, request_timeout: Optional[float] = 300):
    """
     Provisions http session manager with retries.
    :return:
    """
    retry_strategy = Retry(
        total=4,
        status_forcelist=[400, 429, 500, 502, 503, 504],
        method_whitelist=method_list or ["HEAD", "GET", "OPTIONS", "TRACE"],
        backoff_factor=1
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

    :param operation: A method to measure execution time for.
    :return: A tuple of (method_execution_time_ns, method_result)
    """
    result = namedtuple('OperationDuration', ['start', 'end', 'elapsed'])
    result.start = time.monotonic_ns()
    result.end = 0
    result.elapsed = 0
    yield result
    result.end = time.monotonic_ns()
    result.elapsed = result.end - result.start


def chunk_list(value: List[Any], num_chunks: int) -> List[List[Any]]:
    """
     Chunks the provided list into a specified number of chunks. This method is thread-safe.

    :param value: A list to chunk.
    :param num_chunks: Number of chunks to generate.
    :return: A list that has num_chunks lists in it. Total length equals length of the original list.
    """
    chunk_size = math.ceil(len(value) / num_chunks)
    return [value[el_pos:el_pos + chunk_size] for el_pos in range(0, len(value), chunk_size)]


def parallelise(
        func_list: List[Tuple[Callable[[...], Any], List[Any], str]],
        num_threads: Optional[int] = None,
        use_processes: bool = False
) -> Dict[str, concurrent.futures.Future]:
    """
     Lazily parallelises execution of a list of functions. Usage example:

     my_funcs = [(my_func1, [arg1, .., argN], 'task1'),..]
     threads = len(os.sched_getaffinity(0))
     tasks = parallelise(func_list, threads, True)

     task_n_result = tasks['taskN'].result()

     For I/O bound work, set use_processes to True, otherwise overall run time will not improve due to GIL.

    :param func_list: A list of (function, arguments) tuples to parallelise.
    :param num_threads: Maximum number of threads to use. On Linux platforms use len(os.sched_getaffinity(0))
      to get number of threads available to current process
    :param use_processes: Use processes instead of thread for parallelisation. Preferrable for work that depends on GIL release.
    :return: A dictionary of (callable_name, callable_future)
    """
    worker_count = num_threads or (len(os.sched_getaffinity(0)) if sys.platform != "win32" else os.cpu_count())
    with ThreadPoolExecutor(max_workers=worker_count) \
            if not use_processes \
            else ProcessPoolExecutor(max_workers=worker_count) as runner_pool:
        return {
            func_alias: runner_pool.submit(func, *func_args) for func, func_args, func_alias in func_list
        }
