# Delta Lake Operations

Supported API:
- read delta table as `pandas.DataFrame`
- read delta table in batches of a provided size, each batch being `pandas.DataFrame`
- read a subset of columns from delta table
- read and filter a delta table without loading all rows in memory

## Examples usage
Prepare connection and load
### For Azure Datalake Gen2

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

### For AWS Simple Storage Service (S3) or S3-Compatible Storage

```python
import os
from adapta.security.clients import AwsClient
from adapta.security.clients.aws._aws_credentials import EnvironmentAwsCredentials
from adapta.storage.delta_lake import load
import pandas as pd
import pyarrow as pa

# Set up environment variables
os.environ["PROTEUS__AWS_ACCESS_KEY_ID"] = minio_access_key_id
os.environ["PROTEUS__AWS_SECRET_ACCESS_KEY"] = minio_secret_key
os.environ["PROTEUS__AWS_REGION"] = "eu-central-1"
os.environ["PROTEUS__AWS_ENDPOINT"] = "http://example.com"

# Create client
credentials = EnvironmentAwsCredentials()
aws_client = AwsClient(credentials)

# Initialize session
aws_client.initialize_session()

# Creating a delta lake table with sample data
data = {
    'Character': ['Boromir', 'Harry Potter', 'Sherlock Holmes', 'Tony Stark', 'Darth Vader'],
    'Occupation': ['Professional succumber to temptation', 'Wizard', 'Detective', 'Iron Man', 'Sith Lord'],
    'Catchphrase': [
        'One does not simply walk into Mordor.',
        'Expecto Patronum!',
        'Elementary, my dear Watson.',
        'I am Iron Man.',
        'I find your lack of faith disturbing.'
    ]
}

df = pd.DataFrame(data)  # Create a pandas DataFrame from the data
table = pa.Table.from_pandas(df)  # Convert the DataFrame to a PyArrow Table
path_test = '/path/to/store/locally/delta/lake/table'  
deltalake.write_deltalake(path_test, table)  # Write the PyArrow Table to a Delta Lake table

# Save the Delta Lake table to S3 blob storage
s3_client.save_data(path_test, s3_path) 

# Get Iterable[pandas.DataFrame]
batches = load(aws_client, s3_path, batch_size=1000))

# Print each loaded batch
for batch in batches:
    print(batch)
    print("\n---\n")

# The content of the Delta Lake table should be printed in the screen
#         Character  ...                            Catchphrase
# 0          Boromir  ...  One does not simply walk into Mordor.
# 1     Harry Potter  ...                      Expecto Patronum!
# 2  Sherlock Holmes  ...            Elementary, my dear Watson.
# 3       Tony Stark  ...                         I am Iron Man.
# 4      Darth Vader  ...  I find your lack of faith disturbing.
# 
# [5 rows x 3 columns]
# ---
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


