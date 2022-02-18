"""Common utility functions. All of these are imported into __init__.py"""
import time


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
