from adapta.storage.models.aws import S3Path


def test_from_hdfs_path():
    path = S3Path.from_hdfs_path("s3a://bucket/nested/key")
    assert path.bucket == "bucket"
    assert path.path == "nested/key"


def test_to_hdfs_path():
    path = S3Path("bucket", "nested/key").to_hdfs_path()
    assert path == "s3a://bucket/nested/key"
