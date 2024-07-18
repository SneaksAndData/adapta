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
import os
from adapta.security.clients import AwsClient
from adapta.security.clients.aws._aws_credentials import EnvironmentAwsCredentials
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
credentials = EnvironmentAwsCredentials()  # Create AWS credentials
aws_client = AwsClient(credentials) 

# Initialize storage client
s3_client = S3StorageClient.create(auth=aws_client)

# Setting blob S3 path 
s3_path = S3Path.from_hdfs_path('s3a://bucket/folder/path_to_my_blob')

# Save data to S3 path
data = {
    'Character': ['Homer Simpson', 'Michael Scott', 'Ron Swanson', 'Sheldon Cooper', 'Captain Jack Sparrow'],
    'Occupation': ['Nuclear Safety Inspector', 'Regional Manager', 'Parks and Recreation Director', 'Theoretical Physicist', 'Pirate Captain'],
    'Catchphrase': [
        'D\'oh!',
        'I am the World\'s Best Boss.',
        'I\'m a simple man. I like pretty, dark-haired women and breakfast food.',
        'Bazinga!',
        'Why is the rum always gone?'
    ]
}

s3_client.save_data_as_blob(
    data=data, blob_path=s3_path, serialization_format=DictJsonSerializationFormat, overwrite=True
)

# Read blob from S3 path
blob_iterator = s3_client.read_blobs(s3_path, serialization_format=DictJsonSerializationFormat)

# Print read blob 
print(list(blob_iterator))
# The blob's content will be displayed as follows:
# [{'Character': ['Boromir', 'Harry Potter', 'Sherlock Holmes', 'Tony Stark', 'Darth Vader'], 'Occupation': ['Professional succumber to temptation', 'Wizard', 'Detective', 'Iron Man', 'Sith Lord'], 'Catchphrase': ['One does not simply walk into Mordor.', 'Expecto Patronum!', 'Elementary, my dear Watson.', 'I am Iron Man.', 'I find your lack of faith disturbing.']}]

# List all blobs for in the same folder
folder_s3_path = S3Path.from_hdfs_path('s3a://bucket/folder/')
folder_data_path_iterator = s3_client.list_blobs(blob_path=folder_s3_path)

print(list(folder_data_path_iterator))
# The list of blobs will resemble this format:
# [S3Path(bucket='bucket', path='folder/path_to_my_blob', protocol='s3'), S3Path(bucket='bucket', path='folder/other_blob', protocol='s3')]
```

#### Download a single blob:
```python
import os
from adapta.security.clients import AwsClient
from adapta.security.clients.aws._aws_credentials import EnvironmentAwsCredentials
from adapta.storage.models.aws import S3Path
from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.format import DataFrameParquetSerializationFormat

# Set up environment variables
os.environ["PROTEUS__AWS_ACCESS_KEY_ID"] = <aws_access_key_id>
os.environ["PROTEUS__AWS_SESSION_TOKEN"] = <aws_session_token>
os.environ["PROTEUS__AWS_SECRET_ACCESS_KEY"] = <aws_secret_access_key>
os.environ["PROTEUS__AWS_REGION"] = "eu-central-1"
os.environ["PROTEUS__AWS_ENDPOINT"] = "http://example.com"

# Create client
credentials = EnvironmentAwsCredentials()
aws_client = AwsClient(credentials)

# Initialize storage client
s3_client = S3StorageClient.create(auth=aws_client)

# List all blob files in the target folder
s3_folder_path = S3Path.from_hdfs_path('s3a://bucket/folder/')
blob_list = s3_client.list_blobs(blob_path=s3_folder_path)
for blob_details in blob_list:
    print(blob_details)

# A list of blobs and its details will be printed out like the following example:
# {'Key': 'folder/0-c309720b-3577-4211-b403-bbb55083e5c3-0.parquet', 'LastModified': datetime.datetime(2024, 6, 27, 14, 10, 28, 29000, tzinfo=tzutc()), 'ETag': '"29097d7d2d11d49fed28745a674af776"', 'Size': 2067, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d61764'}}
# {'Key': 'folder/_delta_log/00000000000000000000.json', 'LastModified': datetime.datetime(2024, 6, 27, 13, 49, 2, 942000, tzinfo=tzutc()), 'ETag': '"29097d7d2d11d49fed28745a674af776"', 'Size': 2074, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d61764'}}

# Read blobs from the S3 
s3_path_parquet_file = S3Path.from_hdfs_path("'s3a://bucket/path_to_blob_file.parquet")
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
s3_client.download_blobs(s3_folder_path, local_path="/tmp/path/to/download")

# Upon executing the command 'ls /tmp/path/to/download' in your terminal, a list of files be visible:   
# 0-c309720b-3577-4211-b403-bbb55083e5c3-0.parquet  _delta_log
```


#### Copy a single blob to a different location:
```python
import os
from adapta.security.clients import AwsClient
from adapta.security.clients.aws._aws_credentials import EnvironmentAwsCredentials
from adapta.storage.models.aws import S3Path
from adapta.storage.blob.s3_storage_client import S3StorageClient

# Set up environment variables
os.environ["PROTEUS__AWS_ACCESS_KEY_ID"] = <aws_access_key_id>
os.environ["PROTEUS__AWS_SESSION_TOKEN"] = <aws_session_token>
os.environ["PROTEUS__AWS_SECRET_ACCESS_KEY"] = <aws_secret_access_key>
os.environ["PROTEUS__AWS_REGION"] = "eu-central-1"
os.environ["PROTEUS__AWS_ENDPOINT"] = "http://example.com"

# Create client
credentials = EnvironmentAwsCredentials()
aws_client = AwsClient(credentials)

# Initialize storage client
s3_client = S3StorageClient.create(auth=aws_client)

# Copy blob from one location to another in S3
s3_target_blob_path = S3Path.from_hdfs_path('s3a://bucket/path_to_blob_copy')
s3_client.copy_blob(blob_path=s3_path, target_blob_path=s3_target_blob_path)

# List all blob files in the target folder
blob_list = s3_client.list_blobs(blob_path=s3_target_blob_path)
for blob_details in blob_list:
    print(blob_details)

#  The list of blobs and their details will be printed out, mirroring the attributes of the original files::
# {'Key': 'path_to_blob_copy/0-c309720b-3577-4211-b403-bbb55083e5c3-0.parquet', 'LastModified': datetime.datetime(2024, 6, 27, 14, 10, 28, 29000, tzinfo=tzutc()), 'ETag': '"29097d7d2d11d49fed28745a674af776"', 'Size': 2067, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d61764'}}
# {'Key': 'path_to_blob_copy/_delta_log/00000000000000000000.json', 'LastModified': datetime.datetime(2024, 6, 27, 13, 49, 2, 942000, tzinfo=tzutc()), 'ETag': '"29097d7d2d11d49fed28745a674af776"', 'Size': 2074, 'StorageClass': 'STANDARD', 'Owner': {'DisplayName': 'minio', 'ID': '02d61764'}}
```