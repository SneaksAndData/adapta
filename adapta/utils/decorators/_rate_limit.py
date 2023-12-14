"""
Rate limit decorator.
"""

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
