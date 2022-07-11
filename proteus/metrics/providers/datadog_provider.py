"""
  Implementation of a metrics provider for Datadog.
"""
import logging
import os
import sys
from typing import Dict, List, Union, Optional

from datadog import initialize, statsd, api
from datadog_api_client.v1.model.event import Event
from datadog_api_client.v1.model.metric_metadata import MetricMetadata

from proteus.metrics._base import MetricsProvider


class DatadogMetricsProvider(MetricsProvider):
    """
      DogStatsD projection of Proteus MetricsProvider.
    """

    def __init__(self, metric_namespace: str, fixed_tags: Dict[str, str] = None, debug=False):
        self._options = {
            'statsd_host': os.getenv('PROTEUS__DD_STATSD_HOST'),
            'statsd_port': os.getenv('PROTEUS__DD_STATSD_PORT'),
            'api_key': os.getenv('PROTEUS__DD_API_KEY'),
            'app_key': os.getenv('PROTEUS__DD_APP_KEY'),
            'api_host': os.getenv('PROTEUS__DD_API_HOST'),
            'statsd_namespace': metric_namespace,
            'statsd_constant_tags': DatadogMetricsProvider.convert_tags(fixed_tags) if fixed_tags else None
        }

        initialize(**self._options)

        self._api = api

        if debug:
            logging.getLogger("datadog.dogstatsd").addHandler(logging.StreamHandler(sys.stdout))

    @staticmethod
    def convert_tags(tag_dict: Optional[Dict[str, str]]) -> Optional[List[str]]:
        """
         Converts tags dictionary to Datadog tag format.

        :param tag_dict: Dictionary of tags.
        :return: A list of tag_key:tag_value
        """
        if not tag_dict:
            return None
        return [f"{k}:{v}" for k, v in tag_dict.items()]

    def update_metric_metadata(self, metric_name: str, metric_metadata: MetricMetadata) -> None:
        """
          Updates metadata of a published metric in DD.

        :param metric_name: Name of the metric to update.
        :param metric_metadata: Metric metadata to apply.
        :return:
        """

        self._api.metadata.Metadata.update(metric_name=metric_name, **metric_metadata.to_dict())

    def increment(self, metric_name: str, tags: Dict[str, str] = None) -> None:
        statsd.increment(metric=metric_name, tags=DatadogMetricsProvider.convert_tags(tags))

    def decrement(self, metric_name: str, tags: Dict[str, str] = None) -> None:
        statsd.decrement(metric=metric_name, tags=DatadogMetricsProvider.convert_tags(tags))

    def count(self, metric_name: str, metric_value: int, tags: Dict[str, str] = None) -> None:
        raise NotImplementedError

    def gauge(self, metric_name: str, metric_value: Union[int, float], tags: Dict[str, str] = None) -> None:
        statsd.gauge(metric=metric_name, value=metric_value, tags=DatadogMetricsProvider.convert_tags(tags))

    def set(self, metric_name: str, metric_value: Union[str, int, float], tags: Dict[str, str] = None) -> None:
        statsd.set(metric=metric_name, value=metric_value, tags=DatadogMetricsProvider.convert_tags(tags))

    def histogram(self, metric_name: str, metric_value: Union[int, float], tags: Dict[str, str] = None) -> None:
        statsd.histogram(metric=metric_name, value=metric_value, tags=DatadogMetricsProvider.convert_tags(tags))

    def event(self, event_info: Event) -> Dict:
        """
         Creates an event using Datadog Event API. This can be used instead of metrics functions, for example, to report state changes.
        :param event_info: Event information.

        title: title for the new event (string)
        text: event message (string)
        aggregation_key: key by which to group events in event stream (string)
        alert_type: "error", "warning", "info" or "success" (EventAlertType)
        date_happened: when the event occurred. if unset defaults to the current time. (POSIX timestamp) (integer)
        handle: user to post the event as. defaults to owner of the application key used to submit. (string)
        priority: priority to post the event as. ("normal" or "low", defaults to "normal") (string)
        related_event_id: post event as a child of the given event (related_event_id: id)
        tags: tags to post the event with (list of strings)
        host: host to post the event with (string).
        You can leave this empty as this method will always attach hostname to the event.
        device_name: device_name to post the event with (list of strings).

        :return: API response.
        """
        return self._api.Event.create(attach_host_name=True, **event_info.to_dict())
