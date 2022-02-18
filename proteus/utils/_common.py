"""Common utility functions. All of these are imported into __init__.py"""
import time
from typing import List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry


def doze(seconds: int, doze_period_ms: int = 100) -> None:
    """Sleeps for time given in seconds.

    Args:
        seconds: Seconds to doze for
        doze_period_ms: Milliseconds per doze cycle

    Returns: None

    """
    loops = int(seconds * 1000 / doze_period_ms)
    for _ in range(loops):
        time.sleep(doze_period_ms / 1000)


def session_with_retries(method_list: Optional[List[str]] = None):
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

    return http
