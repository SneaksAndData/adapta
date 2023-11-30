# Storage Clients

This module contains storage clients for various cloud/hybrid platforms. Base class is `StorageClient`, concrete implementations reside in respective files, i.e. `azure_storage_client`.

## Usage

In order to init a storage client, you need a respective authentication provider (`SecurityClient`) and a data path:

### Azure examples
#### Read multiple blobs into a pandas Dataframe:
```python
import pandas
from adapta.security.clients import AzureClient
from adapta.storage.models.azure import AdlsGen2Path
from adapta.storage.blob.azure_storage_client import AzureStorageClient
from adapta.storage.models.format import DataFrameParquetSerializationFormat

azure_client = AzureClient()
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

#### Download a single blob:
```python
from adapta.security.clients import AzureClient
from adapta.storage.models.azure import AdlsGen2Path
from adapta.storage.blob.azure_storage_client import AzureStorageClient

azure_client = AzureClient()
adls_path = AdlsGen2Path.from_hdfs_path('abfss://container@account.dfs.core.windows.net/path/to/my/folder/file_name')

# init azure storage client
azure_storage_client = AzureStorageClient(base_client=azure_client, path=adls_path)

# download a file from Azure Storage to local path
local_path = "/local/path"
azure_storage_client.download_blob(adls_path, local_path)
```
Showing downloaded file in terminal
```commandline
cat /local/path/file_name
```

### AWS example
```python
from adapta.security.clients import AwsClient
from adapta.storage.models.aws import S3Path
from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.format import DictJsonSerializationFormat

aws_client = AwsClient()
s3_path = S3Path.from_hdfs_path('s3a://bucket/path/to/my/table')

# init storage client
s3_client = S3StorageClient(base_client=aws_client)

# Save data to S3

s3_client.save_data_as_blob(
    data={"data_value": "2"}, blob_path=s3_path, serialization_format=DictJsonSerializationFormat, overwrite=True
)

# read files from S3
blobs = s3_client.read_blobs(s3_path, serialization_format=DictJsonSerializationFormat)
```
