# Storage Clients

This module contains storage clients for various cloud/hybrid platforms. Base class is `StorageClient`, concrete implementations reside in respective files, i.e. `azure_storage_client`.

## Usage

In order to init a storage client, you need a respective authentication provider (`SecurityClient`) and a data path:

```python
import pandas
from proteus.security.clients import AzureClient
from proteus.storage.models.azure import AdlsGen2Path
from proteus.storage.blob.azure_storage_client import AzureStorageClient
from proteus.storage.models.format import DataFrameParquetSerializationFormat

azure_client = AzureClient(subscription_id='6c5538ce-b24a-4e2a-877f-979ad71287ff')
adls_path = AdlsGen2Path.from_hdfs_path('abfss://container@account.dfs.core.windows.net/path/to/my/table')

# init storage client
azure_storage_client = AzureStorageClient(base_client=azure_client, path=adls_path)

# read a parquet table from Azure Storage

non_partitioned_parquet_table: pandas.DataFrame = pandas.concat(azure_storage_client.read_blobs(
    blob_path=adls_path,
    serialization_format=DataFrameParquetSerializationFormat,
    filter_predicate=lambda b: b.name.endswith('.parquet')  # Ignore non-parquet files that might be present in a folder
))
```

It is also important for the environment you are running in to have datadog agent available on `PROTEUS__DD_STATSD_HOST` address. For our clusters it is always `datadog-statsd.datadog.svc.cluster.local` 

Reporting metrics:

```python
import random
from time import sleep
from proteus.metrics.providers.datadog_provider import DatadogMetricsProvider

provider = DatadogMetricsProvider(metric_namespace='proteus_test')

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
from proteus.metrics.providers.datadog_provider import DatadogMetricsProvider
from datadog_api_client.v1.model.metric_metadata import MetricMetadata

DatadogMetricsProvider.update_metric_metadata(metric_name='my_metric.test', metric_metadata=MetricMetadata(description='best metric!'))
```
