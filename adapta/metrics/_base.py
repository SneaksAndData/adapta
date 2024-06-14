"""
 Metrics integration abstraction.
"""
#  Copyright (c) 2023-2024. ECCO Sneaks & Data
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

from abc import ABC, abstractmethod
from typing import Dict, Optional


class MetricsProvider(ABC):
    """
    Base class for metrics implementations.
    """

    @abstractmethod
    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> None:
        """

        :param metric_name:
        :param tags:
        :return:
        """

    @abstractmethod
    def decrement(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> None:
        """

        :param metric_name:
        :param tags:
        :return:
        """

    @abstractmethod
    def count(self, metric_name: str, metric_value: int, tags: Optional[Dict[str, str]] = None) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def gauge(self, metric_name: str, metric_value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def set(self, metric_name: str, metric_value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    @abstractmethod
    def histogram(self, metric_name: str, metric_value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """

        :param metric_name:
        :param metric_value:
        :param tags:
        :return:
        """

    def event(
        self,
        title: str,
        message: str,
        alert_type: Optional[str] = None,
        aggregation_key: Optional[str] = None,
        source_type_name: Optional[str] = None,
        date_happened: Optional[int] = None,
        priority: Optional[str] = None,
        tags: Optional[Optional[Dict[str, str]]] = None,
        hostname: Optional[str] = None,
    ) -> None:
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
