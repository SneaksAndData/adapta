"""
 Metrics integration abstraction.
"""
from abc import ABC, abstractmethod
from typing import Dict, Optional


class MetricsProvider(ABC):
    """
      Base class for metrics implementations.
    """

    @abstractmethod
    def increment(self, metric_name: str, tags: Optional[Dict[str, str]]) -> None:
        """

        :param metric_name:
        :param tags:
        :return:
        """

    @abstractmethod
    def decrement(self, metric_name: str, tags: Optional[Dict[str, str]]) -> None:
        """

        :param metric_name:
        :param tags:
        :return:
        """

    @abstractmethod
    def count(self, metric_name: str, metric_value: int, tags: Optional[Dict[str, str]]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def gauge(self, metric_name: str, metric_value: float, tags: Optional[Dict[str, str]]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def set(self, metric_name: str, metric_value: float, tags: Optional[Dict[str, str]]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def histogram(self, metric_name: str, metric_value: float, tags: Optional[Dict[str, str]]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    def event(self,
              title: str,
              message: str,
              alert_type: Optional[str] = None,
              aggregation_key: Optional[str] = None,
              source_type_name: Optional[str] = None,
              date_happened: Optional[int] = None,
              priority: Optional[str] = None,
              tags: Optional[Optional[Dict[str, str]]] = None,
              hostname: Optional[str] = None) -> None:
        """
          Send an event to statsd.

        :param title: Event title.
        :param message: Event message.
        :param alert_type: Event type: error, info, warn
        :param aggregation_key: Field to aggregate similar events by.
        :param source_type_name: Event source type.
        :param date_happened: when the event occurred. if unset defaults to the current time. (POSIX timestamp) (integer).
        :param priority: priority to post the event as. ("normal" or "low", defaults to "normal") (string).
        :param tags: Tag mapping for this event.
        :param hostname: Optional hostname.
        :return:
        """
