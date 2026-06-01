# Datadog Metrics Provider

An implementation of a `MetricsProvider` for Datadog platform.

## Usage

### UDP

In order to utilize UDP transport for metrics, you must set the following env variables:

```shell
export PROTEUS__DD_STATSD_HOST=datadog-statsd.datadog.svc.cluster.local
export PROTEUS__DD_STATSD_PORT=8125
```
Note that these are examples, actual service address might differ in your environment.
It is also important for the environment you are running in to have datadog agent available on `PROTEUS__DD_STATSD_HOST` address, and for `PROTEUS__DD_STATSD_PORT` to be reachable - check firewall rules!

### UDS

For UDS you must ensure that Unix Domain Socket file is available on your host. It is best to follow the guide from [Datadog](https://docs.datadoghq.com/developers/dogstatsd/unix_socket/?tab=host), if you need that set up. In addition, note that UDS only becomes available at a certain point of agent boot sequence, hence it is advised to set non-zero `wait_for_socket_timeout_seconds`.

By default, `/var/run/datadog/dsd.socket` path is used for the socket file. Use `PROTEUS__DD_STATSD_SOCKET_PATH` to override it, if needed.

Reporting metrics:

```python
import random
from time import sleep
from adapta.metrics.providers.datadog_provider import DatadogMetricsProvider

provider = DatadogMetricsProvider.uds(metric_namespace='test', wait_for_socket_timeout_seconds=300)

# report a gauge metric
for i in range(0, 100):
    sleep(1)
    provider.gauge(metric_name="test_gauge", metric_value=random.random(), tags={'env': 'test', 'other_tag': f'{i % 10}'})

# report a count metric using increment/decrement
for i in range(0, 100):
    sleep(1)
    if random.random() > 0.4:
        provider.increment(metric_name="test_inc", tags={'env': 'test', 'other_tag': f'{i % 10}'})
    else:
        provider.decrement(metric_name="test_inc", tags={'env': 'test', 'other_tag': f'{i % 10}'})

# report a SET metric
for i in range(0, 100):
    sleep(0.1)
    provider.set(metric_name="test_set", metric_value=f"some-value-{random.randint(0, 100)}", tags={'env': 'test', 'other_tag': f'{i % 10}'})
```

You can also enrich pushed metrics with information like units, description etc.:
```python
from adapta.metrics.providers.datadog_provider import DatadogMetricsProvider
from datadog_api_client.v1.model.metric_metadata import MetricMetadata

DatadogMetricsProvider.update_metric_metadata(metric_name='my_metric.test', metric_metadata=MetricMetadata(description='best metric!'))
```
