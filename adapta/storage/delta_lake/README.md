# Delta Lake Operations

Supported API:
- read delta table as `pandas.DataFrame`
- read delta table in batches of a provided size, each batch being `pandas.DataFrame`
- read a subset of columns from delta table
- read and filter a delta table without loading all rows in memory

## Example usage for Azure Datalake Gen2
Prepare connection and load

```python
import os
from adapta.security.clients import AzureClient
from adapta.storage.models.azure import AdlsGen2Path
from adapta.storage.delta_lake import load

os.environ["PROTEUS__USE_AZURE_CREDENTIAL"] = "1"
azure_client = AzureClient()
adls_path = AdlsGen2Path.from_hdfs_path('abfss://container@account.dfs.core.windows.net/path/to/my/table')

# get Iterable[pandas.DataFrame]
batches = load(azure_client, adls_path, batch_size=1000)
```
## Using the Filtering API.
1. Create generic filter expressions
```python
from adapta.storage.models.filter_expression import FilterField

simple_filter = FilterField("my_column") == "some-value"
combined_filter = (FilterField("my_column") == "some-value") & (FilterField("other_column") == "another-value")
combined_filter_with_collection = (FilterField("my_column") == "something1") & (FilterField("other_column").isin(['else', 'nonexistent']))
complex_filter = (FilterField("my_column") == "something1") | (FilterField("my_other_column") == "else") & (FilterField("another_column") == 123)
```
2. Load and apply the expression
```python
# simple_filtered is of type pandas.DataFrame
simple_filtered = load(azure_client, adls_path, row_filter=simple_expression_pyarrow, columns=["my_column", "my_other_column"])
#     my_column my_other_column
# 0  some-value             123
# 1  some-value   another-value

print(load(azure_client, adls_path, row_filter=combined_filter, columns=["my_column", "my_other_column"]))
#     my_column my_other_column
# 0  some-value   another-value

print(load(azure_client, adls_path, row_filter=combined_filter_with_collection, columns=["my_column", "my_other_column"]))
#     my_column my_other_column
# 0  something1            else
# 1  something1     nonexistent

print(load(azure_client, adls_path, row_filter=complex_filter, columns=["my_column", "my_other_column", "another_column"]))
#     my_column my_other_column  another_column
# 0  something1            else               1
# 1  something1     nonexistent               2
# 2  something1    nonexistent1             123

```
# Using with Hive paths
```python
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


