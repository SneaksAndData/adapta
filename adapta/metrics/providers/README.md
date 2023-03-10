# Datadog Metrics Provider

An implementation of a `MetricsProvider` for Datadog platform.

## Usage

Several environment variables must be set before you can use this provider:

```shell
export PROTEUS__DD_STATSD_HOST=datadog-statsd.datadog.svc.cluster.local
export PROTEUS__DD_STATSD_PORT=8125
export PROTEUS__DD_API_KEY=<api key>
export PROTEUS__DD_APP_KEY=<app key>
export PROTEUS__DD_API_HOST=api.datadoghq.eu
```

It is also important for the environment you are running in to have datadog agent available on `PROTEUS__DD_STATSD_HOST` address. For our clusters it is always `datadog-statsd.datadog.svc.cluster.local` 

Reporting metrics:

```python
import random
from time import sleep
from adapta.metrics.providers.datadog_provider import DatadogMetricsProvider

provider = DatadogMetricsProvider(metric_namespace='test')

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
