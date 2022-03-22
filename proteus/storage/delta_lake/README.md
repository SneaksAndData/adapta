# Delta Lake Operations

Supported API:
- read delta table as `pandas.DataFrame`
- read delta table in batches of a provided size, each batch being `pandas.DataFrame`
- read a subset of columns from delta table
- read and filter a delta table without loading all rows in memory

## Example usage for Azure Datalake Gen2

```python
from proteus.security.clients import AzureClient
from proteus.storage.models.azure import AdlsGen2Path
from proteus.storage.delta_lake import load
from pyarrow.dataset import field as pyarrow_field

# prepare connection
azure_client = AzureClient(subscription_id='6c5538ce-b24a-4e2a-877f-979ad71287ff')
adls_path = AdlsGen2Path.from_hdfs_path('abfss://container@account.dfs.core.windows.net/path/to/my/table')

# get Iterable[pandas.DataFrame]
batches = load(azure_client, adls_path, batch_size=1000)
	
# create a filter and apply it
filter = (pyarrow_field("my_column") == "some-value") # case sensitive!
filtered = load(azure_client, adls_path, row_filter=filter, columns=["my_column", "my_other_column"])

# filtered is of type pandas.DataFrame
```