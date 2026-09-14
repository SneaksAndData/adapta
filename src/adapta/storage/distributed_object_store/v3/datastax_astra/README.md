# Astra DB Client

`AstraClient` connects to DataStax Astra DB using a secure connect bundle and Astra application token. It inherits all query, filter, and upsert functionality from [`CassandraClient`](../cassandra_client/README.md) while adding native support for vector search (ANN).

---

## Creating an AstraClient

```python
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient

client = AstraClient(
    client_name="my_app",
    keyspace="my_keyspace",
    secure_connect_bundle_bytes="<base64-encoded secure connect bundle>",
    client_id="<Astra token Client ID>",
    client_secret="<Astra token Client Secret>",
)

with client:
    # Query data into pandas / polars DataFrames via raw CQL
    df = client.get_entities_raw("SELECT * FROM my_keyspace.my_table LIMIT 10").to_pandas()
    print(df)
```

See [CassandraClient README](../cassandra_client/README.md) for full details on data modeling, entity filtering, and batch upserts.

---

## Vector Search (ANN)

`AstraClient` supports vector similarity search over Astra vector columns:

```python
from dataclasses import dataclass, field
from adapta.storage.distributed_object_store.v3.datastax_astra import (
    AstraClient,
    SimilarityFunction,
)


@dataclass
class DocumentEntity:
    doc_id: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    content: str
    embedding: list[float] = field(metadata={"is_vector_enabled": True})


with client:
    results_df = client.ann_search(
        entity_type=DocumentEntity,
        vector_to_match=[0.1, 0.2, 0.3],
        similarity_function=SimilarityFunction.DOT_PRODUCT,
        num_results=5,
    ).to_pandas()

    print(results_df)
```