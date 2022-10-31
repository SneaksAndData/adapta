"""Common utility functions. All of these are imported into __init__.py"""
import contextlib
import time
from collections import namedtuple
from functools import partial
from typing import List, Optional, Dict

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
        status_forcelist=[429, 500, 502, 503, 504],
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
    try:
        result.start = time.monotonic_ns()
        result.end = 0
        result.elapsed = 0
        yield result
    finally:
        result.end = time.monotonic_ns()
        result.elapsed = result.end - result.start
        return result
