# Cassandra Client

`CassandraClient` is the base client class for connecting to and interacting with Apache Cassandra clusters. It provides high-level APIs for CQL queries, object-relational mapping with Python `dataclass`, filtering, batch upserts, and exporting results directly to **pandas** or **polars** DataFrames via `MetaFrame`.

Adapta provides concrete client implementations:
- **`VanillaCassandraClient`**: For standard / local Apache Cassandra clusters ([Vanilla README](../vanilla_cassandra/README.md))
- **`AstraClient`**: For DataStax Astra DB ([Astra README](../datastax_astra/README.md))
- **`AwsKeyspaceClient`**: For Amazon Keyspaces ([Keyspaces README](../aws_keyspaces/README.md))

---

## Connecting with `VanillaCassandraClient`

Use `VanillaCassandraClient` to connect to local or standard Cassandra instances:

```python
from adapta.storage.distributed_object_store.v3.vanilla_cassandra import VanillaCassandraClient

client = VanillaCassandraClient(
    client_name="local_dev",
    keyspace="my_keyspace",
    contact_points=["127.0.0.1"],  # defaults to ["127.0.0.1"]
    port=9042,                      # defaults to 9042
    username=None,                  # optional username for PlainTextAuthProvider
    password=None,                  # optional password
)
```

---

## Usage Examples

### 1. Data Model Definition

Define your schema using Python dataclasses. Metadata flags denote primary and partition keys:

```python
from dataclasses import dataclass, field


@dataclass
class UserEntity:
    user_id: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    user_name: str
    email: str
    age: int
```

### 2. Reading Data into DataFrames

`CassandraClient` provides context manager support (`with client:`) which automatically connects and disconnects.

```python
import pandas as pd
import polars as pl

client = VanillaCassandraClient(
    client_name="local_dev",
    keyspace="my_keyspace",
    contact_points=["127.0.0.1"],
    port=9042,
)

with client:
    # --- Option A: Raw CQL queries ---
    meta_frame = client.get_entities_raw("SELECT * FROM my_keyspace.user_entity LIMIT 100")

    # Convert results directly to pandas or polars
    df_pandas: pd.DataFrame = meta_frame.to_pandas()
    df_polars: pl.DataFrame = meta_frame.to_polars()
    print(df_pandas.head())

    # --- Option B: Query with a typed model using filter_entities ---
    filtered_meta_frame = client.filter_entities(
        model_class=UserEntity,
        table_name="user_entity",
        key_column_filter_values=[{"user_id": "user-123"}],
        select_columns=["user_id", "user_name", "email"],
    )
    df_filtered = filtered_meta_frame.to_pandas()
    print(df_filtered)

    # --- Option C: Read a single entity as a dict ---
    single_entity = client.get_entity("user_entity")
    print(single_entity)
```

### 3. Writing Data

#### Upsert a Single Entity
```python
with client:
    user = UserEntity(
        user_id="user-123",
        user_name="John Doe",
        email="john@example.com",
        age=30,
    )
    client.upsert_entity(entity=user, table_name="user_entity")
```

#### Batch Upsert
```python
rows = [
    {"user_id": "user-1", "user_name": "Alice", "email": "alice@example.com", "age": 25},
    {"user_id": "user-2", "user_name": "Bob", "email": "bob@example.com", "age": 28},
]

with client:
    client.upsert_batch(
        entities=rows,
        entity_type=UserEntity,
        table_name="user_entity",
        batch_size=500,
    )
```

#### Delete an Entity
```python
with client:
    client.delete_entity(entity=user, table_name="user_entity")
```
