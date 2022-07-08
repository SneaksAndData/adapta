"""Common utility functions. All of these are imported into __init__.py"""
import time
from typing import List, Optional
from argparse import ArgumentParser, Namespace

import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from proteus.utils._models import CrystalEntrypointArguments


def doze(seconds: int, doze_period_ms: int = 100) -> int:
    """Sleeps for time given in seconds.

    Args:
        seconds: Seconds to doze for
        doze_period_ms: Milliseconds per doze cycle

    Returns: Time elapsed in nanoseconds

    """
    loops = int(seconds * 1000 / doze_period_ms)
    start = time.time_ns()
    for _ in range(loops):
        time.sleep(doze_period_ms / 1000)

    return time.time_ns() - start


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


def add_crystal_args(parser: Optional[ArgumentParser] = None) -> ArgumentParser:
    """
    Add Crystal arguments to the command line argument parser.
    Notice that you need to add these arguments before calling `parse_args`.
    If no parser is provided, a new will be instantiated.

    :param parser: Existing argument parser.
    :return: The existing argument parser (if provided) with Crystal arguments added.
    """
    if parser is None:
        parser = ArgumentParser()

    parser.add_argument('--sas-uri', required=True, type=str, help='SAS URI for input data')
    parser.add_argument('--request-id', required=True, type=str, help='ID of the task')
    parser.add_argument('--results-receiver', required=True, type=str, help='HTTP(s) endpoint to which output SAS URI is passed')
    parser.add_argument('--results-receiver-user', required=False, type=str, help='User for results receiver (authentication)')
    parser.add_argument('--results-receiver-password', required=False, type=str, help='Password for results receiver (authentication)')
    parser.add_argument('--sign-result', dest='sign_result', required=False, action='store_true')
    parser.set_defaults(sign_result=False)

    return parser


def extract_crystal_args(args: Namespace) -> CrystalEntrypointArguments:
    """
    Extracts parsed Crystal arguments and returns as a dataclass.
    :param args: Parsed arguments.
    :return: CrystalArguments object
    """
    return CrystalEntrypointArguments(
        sas_uri=args.sas_uri,
        request_id=args.request_id,
        results_receiver=args.results_receiver,
        sign_result=args.sign_result
    )
