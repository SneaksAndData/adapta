import os

from adapta.security.clients import AwsClient
from adapta.security.clients.aws import ExplicitAwsCredentials
from adapta.storage.blob.s3_storage_client import S3StorageClient
from adapta.storage.models.aws import S3Path
from adapta.storage.models.format import UnitSerializationFormat, DictJsonSerializationFormat


def test_can_read():
    key = "logs/dag_id=azureAdUsersListing/run_id=scheduled__2023-09-28T08:23:22.159577+00:00/task_id=compact-table-users/attempt=3.log"
    bucket = "esd-airflow-dev-0-log-archive"
    path = S3Path(bucket, key)
    base_client = AwsClient(
        ExplicitAwsCredentials(
            access_key="r1YbY/UnSqAgJ+6oVXihP+Hkann9K3qMI6qTN7nY",
            access_key_id="AKIA557RQRTSKAQR6NHW",
            region="eu-central-1",
        )
    )
    base_client.initialize_session()
    s3_client = S3StorageClient(base_client=base_client)
    result = list(s3_client.read_blobs(path, serialization_format=UnitSerializationFormat))

    save_path = S3Path(bucket, "logs/test.log")
    s3_client.delete_blob(save_path)
    s3_client.save_data_as_blob(
        data={"request_id": "1"}, blob_path=save_path, serialization_format=DictJsonSerializationFormat, overwrite=False
    )
    s3_client.save_data_as_blob(
        data={"request_id": "2"}, blob_path=save_path, serialization_format=DictJsonSerializationFormat, overwrite=True
    )
    result = next(s3_client.read_blobs(save_path, serialization_format=DictJsonSerializationFormat))
    assert result["request_id"] == "2"
