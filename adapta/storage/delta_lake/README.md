# Delta Lake Operations

Supported API:
- read delta table as `pandas.DataFrame`
- read delta table in batches of a provided size, each batch being `pandas.DataFrame`
- read a subset of columns from delta table
- read and filter a delta table without loading all rows in memory

## Example usage for Azure Datalake Gen2

```python
import os
import datetime
from adapta.security.clients import AzureClient
from adapta.storage.models.azure import AdlsGen2Path
from adapta.storage.models.hive import HivePath
from adapta.storage.delta_lake import load, load_cached
from adapta.logs import SemanticLogger
from adapta.storage.cache.redis_cache import RedisCache
from adapta.storage.models.filter_expression import FilterField, ArrowFilterExpression, compile_expression

# prepare connection
azure_client = AzureClient(subscription_id='6c5538ce-b24a-4e2a-877f-979ad71287ff')
adls_path = AdlsGen2Path.from_hdfs_path('abfss://container@account.dfs.core.windows.net/path/to/my/table')

# get Iterable[pandas.DataFrame]
batches = load(azure_client, adls_path, batch_size=1000)

# create generic filter expressions
simple_filter = FilterField[str]("my_column") == "some-value"
combined_filter = (FilterField[str]("my_column") == "some-value") & (FilterField[str]("other_column") == "another-value")
combined_filter_with_collection = (FilterField[str]("my_column") == "something1") & (FilterField[str]("other_column").isin(['else', 'nonexistent']))
complex_filter = (FilterField[str]("my_column") == "something1") | (FilterField[str]("other_column") == "else") & (FilterField[int]("another_column") == 123)

# compile filters for PyArrow
simple_expression_pyarrow = compile_expression(simple_filter, ArrowFilterExpression)
combined_expression_pyarrow = compile_expression(combined_filter, ArrowFilterExpression)
combined_expression_with_collection_pyarrow = compile_expression(combined_filter_with_collection, ArrowFilterExpression)
complex_expression_pyarrow = compile_expression(complex_filter, ArrowFilterExpression)

# apply filter 
simple_filtered = load(azure_client, adls_path, row_filter=simple_expression_pyarrow, columns=["my_column", "my_other_column"])

# simple_filtered is of type pandas.DataFrame

# using with Hive paths
logger: SemanticLogger  # review proteus.logs readme to learn how to construct a logger instance
os.environ['PROTEUS__HIVE_USER'] = 'delamain'
os.environ['PROTEUS__HIVE_PASSWORD'] = 'secret'
hive_path = HivePath.from_hdfs_path(
    "hive://sqlserver@myserver.database.windows.net:1433/sparkdatalake/bronze/bronze_table")

adls_path2 = AdlsGen2Path.from_hdfs_path(hive_path.get_physical_path(logger=logger))

# get Iterable[pandas.DataFrame]
batches2 = load(azure_client, adls_path2, batch_size=1000)

# read data using Redis Cache, improves read time by a factor of >10 on single-node Redis.
# for big tables, choose bigger batch sizes to speed up cache population. General rule:
# batch_size = row_count / 10
# if there is no cache hit, load_cached() will fallback to load() behaviour
r_cache = RedisCache(host="esd-superset-test.redis.cache.windows.net", database_number=1)
os.environ['PROTEUS__CACHE_REDIS_PASSWORD'] = '...'
read_raw = load_cached(azure_client, adls_path, r_cache, row_filter=filter,
                       cache_expires_after=datetime.timedelta(minutes=15), batch_size=int(1e6))
```