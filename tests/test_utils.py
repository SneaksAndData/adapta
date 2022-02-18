import time

import pytest
from proteus.utils import doze


@pytest.mark.parametrize('sleep_period,doze_interval', [(1, 50), (2, 10)])
def test_doze(sleep_period: int, doze_interval: int):
    start_ts = time.process_time_ns() / 1e9
    doze(sleep_period, doze_interval)
    time_passed = time.process_time_ns() / 1e9 - start_ts

    assert int(time_passed) == sleep_period
