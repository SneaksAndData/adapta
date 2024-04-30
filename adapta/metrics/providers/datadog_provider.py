"""
  Implementation of a metrics provider for Datadog.
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

import logging
import os
import sys
from enum import Enum
from typing import Dict, Union, Optional

from datadog import initialize, statsd, api
from datadog_api_client.v1.model.metric_metadata import MetricMetadata

from adapta.metrics._base import MetricsProvider
from adapta.utils import convert_datadog_tags


class EventAlertType(Enum):
    """
    Wrapper for alert_type value set in Events API.
    """

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"
    SUCCESS = "success"


class DatadogMetricsProvider(MetricsProvider):
    """
    DogStatsD projection of MetricsProvider.
    """

    def __init__(self, metric_namespace: str, fixed_tags: Dict[str, str] = None, debug=False, **options):
        self._options = {
            "statsd_namespace": metric_namespace,
            "statsd_constant_tags": convert_datadog_tags(fixed_tags) if fixed_tags else None,
        } | options

        initialize(**self._options)

        self._api = api

        if debug:
            logging.getLogger("datadog.dogstatsd").addHandler(logging.StreamHandler(sys.stdout))

    @classmethod
    def udp(cls, metric_namespace: str, fixed_tags: Dict[str, str] = None, debug=False):
        """
        Enables sending metrics via UDP
        """
        return cls(
            metric_namespace=metric_namespace,
            fixed_tags=fixed_tags,
            debug=debug,
            statsd_host=os.getenv("PROTEUS__DD_STATSD_HOST"),
            statsd_port=os.getenv("PROTEUS__DD_STATSD_PORT"),
            api_key=os.getenv("PROTEUS__DD_API_KEY"),
            app_key=os.getenv("PROTEUS__DD_APP_KEY"),
            api_host=os.getenv("PROTEUS__DD_API_HOST"),
        )

    @classmethod
    def uds(cls, metric_namespace: str, fixed_tags: Dict[str, str] = None, debug=False):
        """
        Enables sending metrics over UDS (Unix Domain Socket).
        You must have dsdsocket path mounted on your system in order to use this mode.
        Refer to https://docs.datadoghq.com/developers/dogstatsd/unix_socket/?tab=kubernetes
        """
        return cls(
            metric_namespace=metric_namespace,
            fixed_tags=fixed_tags,
            debug=debug,
            statsd_socket_path=os.getenv("PROTEUS__DD_STATSD_SOCKET_PATH") or "/var/run/datadog/dsd.socket",
        )

    def update_metric_metadata(self, metric_name: str, metric_metadata: MetricMetadata) -> None:
        """
          Updates metadata of a published metric in DD.

        :param metric_name: Name of the metric to update.
        :param metric_metadata: Metric metadata to apply.
        :return:
        """

        self._api.metadata.Metadata.update(metric_name=metric_name, **metric_metadata.to_dict())

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> None:
        statsd.increment(metric=metric_name, tags=convert_datadog_tags(tags))

    def decrement(self, metric_name: str, tags: Optional[Dict[str, str]] = None) -> None:
        statsd.decrement(metric=metric_name, tags=convert_datadog_tags(tags))

    def count(self, metric_name: str, metric_value: int, tags: Optional[Dict[str, str]] = None) -> None:
        raise NotImplementedError

    def gauge(
        self,
        metric_name: str,
        metric_value: Union[int, float],
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        statsd.gauge(metric=metric_name, value=metric_value, tags=convert_datadog_tags(tags))

    def set(
        self,
        metric_name: str,
        metric_value: Union[str, int, float],
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        statsd.set(metric=metric_name, value=metric_value, tags=convert_datadog_tags(tags))

    def histogram(
        self,
        metric_name: str,
        metric_value: Union[int, float],
        tags: Optional[Dict[str, str]] = None,
    ) -> None:
        statsd.histogram(metric=metric_name, value=metric_value, tags=convert_datadog_tags(tags))

    def event(
        self,
        title: str,
        message: str,
        alert_type: Optional[str] = EventAlertType.INFO.value,
        aggregation_key: Optional[str] = None,
        source_type_name: Optional[str] = None,
        date_happened: Optional[int] = None,
        priority: Optional[str] = None,
        tags: Optional[Optional[Dict[str, str]]] = None,
        hostname: Optional[str] = None,
    ) -> None:
        statsd.event(
            title=title,
            message=message,
            alert_type=alert_type,
            aggregation_key=aggregation_key,
            source_type_name=source_type_name,
            date_happened=date_happened,
            priority=priority,
            tags=convert_datadog_tags(tags),
            hostname=hostname,
        )
