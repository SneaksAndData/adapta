"""
  Dev mode metrics provider.
"""
from typing_extensions import final

from adapta.metrics import MetricsProvider


@final
class VoidMetricsProvider(MetricsProvider):
    """
    Metrics provider that sends data into the void. Useful for testing.
    """

    def increment(self, metric_name: str, tags: dict[str, str] | None = None) -> None:
        pass

    def decrement(self, metric_name: str, tags: dict[str, str] | None = None) -> None:
        pass

    def count(self, metric_name: str, metric_value: int, tags: dict[str, str] | None = None) -> None:
        pass

    def gauge(self, metric_name: str, metric_value: float, tags: dict[str, str] | None = None) -> None:
        pass

    def set(self, metric_name: str, metric_value: float, tags: dict[str, str] | None = None) -> None:
        pass

    def histogram(self, metric_name: str, metric_value: float, tags: dict[str, str] | None = None) -> None:
        pass
