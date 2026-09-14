# AWS Keyspaces Client

`AwsKeyspaceClient` connects to Amazon Keyspaces (for Apache Cassandra) using AWS SigV4 (IAM) authentication and TLS. It inherits all query, filter, and upsert functionality from [`CassandraClient`](../cassandra_client/README.md).

---

## Creating an AwsKeyspaceClient

Ensure your environment has AWS credentials configured (via `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`, AWS profile, or IAM role).

```python
from adapta.storage.distributed_object_store.v3.aws_keyspaces import AwsKeyspaceClient

client = AwsKeyspaceClient(
    client_name="my_keyspaces_app",
    keyspace="my_keyspace",
    region="eu-central-1",
    profile_name=None # optionally add your AWS CLI profile name
)

with client:
    # Query data into pandas / polars DataFrames via raw CQL
    df = client.get_entities_raw("SELECT * FROM my_keyspace.my_table LIMIT 10").to_pandas()
    print(df)
```

See [CassandraClient README](../cassandra_client/README.md) for full details on data modeling, entity filtering, and batch upserts.
