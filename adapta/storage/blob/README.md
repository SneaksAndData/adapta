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
local_path = "/tmp/path"
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
s3_client = S3StorageClient.create(base_client=aws_client)

# Save data to S3
s3_client.save_data_as_blob(
    data={"data_value": "2"}, blob_path=s3_path, serialization_format=DictJsonSerializationFormat, overwrite=True
)

# read files from S3
blobs = s3_client.read_blobs(s3_path, serialization_format=DictJsonSerializationFormat)
```

#### Download a single blob:
```python
from adapta.security.clients import AwsClient
from adapta.storage.models.aws import S3Path
from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.format import DictJsonSerializationFormat

# Set up environment variables
os.environ["PROTEUS__AWS_ACCESS_KEY_ID"] = <aws_access_key_id>
os.environ["PROTEUS__AWS_SESSION_TOKEN"] = <aws_session_token>
os.environ["PROTEUS__AWS_SECRET_ACCESS_KEY"] = <aws_secret_access_key>
os.environ["PROTEUS__AWS_REGION"] = "eu-central-1"
os.environ["PROTEUS__AWS_ENDPOINT"] = "http://example.com"

# Create client
credentials = EnvironmentAwsCredentials()
aws_client = AwsClient(credentials)

# Target path for copy_blob
blob_path = "path/to/blob.file" "# It can be either a 'blob.file' or a 'folder/'
s3_path = S3Path.from_hdfs_path(blob_path)

# Init storage client
s3_client = S3StorageClient.create(base_client=aws_client)

# Save data to S3
s3_client.save_data_as_blob(
    data={"data_value": "very_important_data"}, blob_path=s3_path, serialization_format=DictJsonSerializationFormat, overwrite=True
)

# List all blob files in an S3 storage path
blob_list = s3_client.list_blobs(s3_path)
for blob_details in blob_list:
    print(blob_details)

# A list of blobs and its details will be printed out like the following example:
# {'Key': 'data/0-c309720b-3577-4211-b403-bbb55083e5c3-0.parquet', 'LastModified': datetime.datetime(2024, 6, 27, 14, 10, 28, 29000, tzinfo=tzutc()), 'ETag': '"29097d7d2d11d49fed28745a674af776"', 'Size': 2067, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d61764'}}
# {'Key': 'data/_delta_log/00000000000000000000.json', 'LastModified': datetime.datetime(2024, 6, 27, 13, 49, 2, 942000, tzinfo=tzutc()), 'ETag': '"29097d7d2d11d49fed28745a674af776"', 'Size': 2074, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d61764'}}

# Read blobs from the S3 path
s3_path_parquet_file = S3Path.from_hdfs_path("path/to/blob.file.parquet")
blobs = s3_client.read_blobs(s3_path_parquet_file, serialization_format=DataFrameParquetSerializationFormat)
# Print blobs content
print(list(blobs))

# The content of the blob should be printed in the screen
# [         Character  ...                            Catchphrase
# 0          Boromir  ...  One does not simply walk into Mordor.
# 1     Harry Potter  ...                      Expecto Patronum!
# 2  Sherlock Holmes  ...            Elementary, my dear Watson.
# 3       Tony Stark  ...                         I am Iron Man.
# 4      Darth Vader  ...  I find your lack of faith disturbing.


# Downloads blobs from the S3 storage path to the provided local path, as if you were navigating within the S3 path.
s3_client.download_blobs(s3_path, local_path="/tmp/path/to/download")

# Upon executing the command 'ls /tmp/path/to/download' in your terminal, a list of files be visible:   
# 0-c309720b-3577-4211-b403-bbb55083e5c3-0.parquet  _delta_log

# Copy blob from one location to another in S3
target_blob_path='s3a://path/to/blob_copy/'
s3_target_blob_path = S3Path.from_hdfs_path(target_blob_path)
s3_client.copy_blob(blob_path=s3_path, target_blob_path=s3_target_blob_path, doze_period_ms=1000) # Time in ms between files being copied
```