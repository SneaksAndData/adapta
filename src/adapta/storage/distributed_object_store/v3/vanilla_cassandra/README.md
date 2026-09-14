# Vanilla Cassandra Client

`VanillaCassandraClient` connects to standard or local Apache Cassandra clusters (such as ScyllaDB or local Cassandra) using contact points, port, optional plain-text authentication, and optional TLS. It inherits all query, filter, and upsert functionality from [`CassandraClient`](../cassandra_client/README.md).

---

## Creating a `VanillaCassandraClient`

```python
from adapta.storage.distributed_object_store.v3.vanilla_cassandra import VanillaCassandraClient

client = VanillaCassandraClient(
    client_name="local_dev",
    keyspace="my_keyspace",
    contact_points=["127.0.0.1"],  # defaults to ["127.0.0.1"]
    port=9042,  # defaults to 9042
    username=None,  # optional username for PlainTextAuthProvider
    password=None,  # optional password
)

with client:
    # Query data into pandas / polars DataFrames via raw CQL
    df = client.get_entities_raw("SELECT * FROM my_keyspace.my_table LIMIT 10").to_pandas()
    print(df)
```

See [CassandraClient README](../cassandra_client/README.md) for full details on data modeling, entity filtering, and batch upserts.
