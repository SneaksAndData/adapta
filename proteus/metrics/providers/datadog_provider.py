import os
from typing import Dict

from datadog import initialize, statsd
from datadog_api_client import Configuration, ApiClient
from datadog_api_client.v1.api.metrics_api import MetricsApi
from datadog_api_client.v1.model.metric_metadata import MetricMetadata

from proteus.metrics._base import MetricsProvider


class DataDogMetricsProvider(MetricsProvider):
    def __init__(self, metric_namespace: str, fixed_tags: Dict[str, str] = None):
        self._options = {
            'statsd_host': os.getenv('PROTEUS__DD_STATSD_HOST'),
            'statsd_port': os.getenv('PROTEUS__DD_STATSD_PORT'),
            'api_key': os.getenv('PROTEUS__DD_API_KEY'),
            'app_key': os.getenv('PROTEUS__DD_APP_KEY'),
            'api_host': os.getenv('PROTEUS__DD_API_HOST'),
            'statsd_namespace': metric_namespace,
            'statsd_constant_tags': [f"{k}:v" for k, v in fixed_tags.items()] if fixed_tags else None
        }

        initialize(**self._options)

    @staticmethod
    def update_metric_metadata(metric_name: str, metric_metadata: MetricMetadata) -> None:
        """
          Updates metadata of a published metric in DD.

        :param metric_name: Name of the metric to update.
        :param metric_metadata: Metric metadata to apply.
        :return:
        """
        configuration = Configuration(
            api_key=os.getenv('PROTEUS__DD_API_KEY'),
            host=os.getenv('PROTEUS__DD_API_HOST')
        )

        with ApiClient(configuration) as api_client:
            MetricsApi(api_client) \
                .update_metric_metadata(metric_name=metric_name, body=metric_metadata)

    def increment(self, metric_name: str, tags: Dict[str, str]) -> None:
        statsd.increment(metric=metric_name, tags=tags)

    def decrement(self, metric_name: str, tags: Dict[str, str]) -> None:
        statsd.decrement(metric=metric_name, tags=tags)

    def count(self, metric_name: str, metric_value: int, tags: Dict[str, str]) -> None:
        raise NotImplemented

    def gauge(self, metric_name: str, metric_value: float, tags: Dict[str, str]) -> None:
        statsd.gauge(metric=metric_name, value=metric_value, tags=tags)

    def set(self, metric_name: str, metric_value: float, tags: Dict[str, str]) -> None:
        statsd.set(metric=metric_name, value=metric_value, tags=tags)

    def histogram(self, metric_name: str, metric_value: float, tags: Dict[str, str]) -> None:
        statsd.histogram(metric=metric_name, value=metric_value, tags=tags)
