"""
 Metrics integration abstraction.
"""
from abc import ABC, abstractmethod
from typing import Dict


class MetricsProvider(ABC):
    """
      Base class for metrics implementations.
    """

    @abstractmethod
    def increment(self, metric_name: str, tags: Dict[str, str]) -> None:
        """

        :param metric_name:
        :param tags:
        :return:
        """

    @abstractmethod
    def decrement(self, metric_name: str, tags: Dict[str, str]) -> None:
        """

        :param metric_name:
        :param tags:
        :return:
        """

    @abstractmethod
    def count(self, metric_name: str, metric_value: int, tags: Dict[str, str]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def gauge(self, metric_name: str, metric_value: float, tags: Dict[str, str]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def set(self, metric_name: str, metric_value: float, tags: Dict[str, str]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def histogram(self, metric_name: str, metric_value: float, tags: Dict[str, str]) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """
