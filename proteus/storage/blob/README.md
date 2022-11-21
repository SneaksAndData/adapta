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