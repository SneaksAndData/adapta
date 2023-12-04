from datetime import datetime

from adapta.utils import rate_limit, doze


def test_rate_limit_decorator():
    count = 0

    @rate_limit(limit="1 per second")
    def increment():
        nonlocal count
        count += 1

    start = datetime.now()
    for _ in range(0, 10):
        increment()

    assert (datetime.now() - start).seconds > 5
    assert count == 10


def test_delay_func():
    count = 0
    delay_calls = 0

    def custom_delay():
        nonlocal delay_calls
        delay_calls += 1
        return doze(2)

    @rate_limit(limit="1 per second", delay_func=custom_delay)
    def increment():
        nonlocal count
        count += 1

    start = datetime.now()
    for _ in range(0, 5):
        increment()

    assert (datetime.now() - start).seconds > 5
    assert count == 5
    assert delay_calls > 2
