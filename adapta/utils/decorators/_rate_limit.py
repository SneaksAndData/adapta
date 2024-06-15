"""
Rate limit decorator.
"""
from enum import Enum
from functools import wraps
from typing import Callable, Optional

from limits.storage import storage_from_string
from limits.strategies import STRATEGIES
from limits.util import parse

from adapta.utils._common import doze


def _default_delay_func() -> None:
    doze(1)


class RateLimitStrategy(Enum):
    """
    Rate limit strategies enumeration.
    """

    FIXED_WINDOW = "fixed-window"
    FIXED_WINDOW_ELASTIC_EXPIRY = "fixed-window-elastic-expiry"
    MOVING_WINDOW = "moving-window"


def rate_limit(
    _func: Callable = None,
    *,
    limit: str,
    strategy: Optional[RateLimitStrategy] = RateLimitStrategy.MOVING_WINDOW,
    delay_func: Callable[[], int] = _default_delay_func
) -> Callable:
    """
    Rate limit decorator.
    :param: limit: the limit string to parse (eg: "100 per hour", "1/second", ...)
    :param: strategy: the strategy to use (default: MovingWindow)
    :param: delay_func: the delay function to use (default: doze(1))
    :param: _func: the function to decorate
    :return: the decorator function
    """

    def decorator(func):
        rate_limiter = STRATEGIES[strategy.value](storage=storage_from_string("memory://"))

        @wraps(func)
        def wrapper(*args, **kwargs):
            while not rate_limiter.hit(parse(limit)):
                delay_func()
            return func(*args, **kwargs)

        return wrapper

    if _func is None:
        return decorator
    return decorator(_func)
