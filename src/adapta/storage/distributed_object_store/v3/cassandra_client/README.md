# Cassandra Client

`CassandraClient` is the base client class for connecting to and interacting with Apache Cassandra clusters. It provides high-level APIs for CQL queries, object-relational mapping with Python `dataclass`, filtering, batch upserts, and exporting results directly to **pandas** or **polars** DataFrames via `MetaFrame`.

Adapta provides concrete client implementations for:
- Local / generic Cassandra clusters (by subclassing `CassandraClient`)
- **DataStax Astra DB**: [AstraClient](../datastax_astra/README.md)
- **AWS Keyspaces**: [AwsKeyspaceClient](../aws_keyspaces/README.md)

---

## Defining a Local Cassandra Client

To connect to a local or standard Cassandra cluster, create a subclass of `CassandraClient`:

```python
import ssl
from cassandra.auth import AuthProvider, PlainTextAuthProvider
from adapta.storage.distributed_object_store.v3.cassandra_client import (
    CassandraClient,
    CassandraClientConfiguration,
)


class LocalCassandraClient(CassandraClient):
    def __init__(
        self,
        client_name: str,
        keyspace: str,
        contact_points: list[str] | None = None,
        port: int = 9042,
        username: str | None = None,
        password: str | None = None,
        client_config: CassandraClientConfiguration | None = None,
    ) -> None:
        super().__init__(client_name, keyspace, client_config)
        self._contact_points = contact_points or ["127.0.0.1"]
        self._port = port
        self._username = username
        self._password = password

    def _get_port(self) -> int | None:
        return self._port

    def _get_contact_points(self) -> list[str] | None:
        return self._contact_points

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        return None

    def _get_auth_provider(self) -> AuthProvider | None:
        if self._username and self._password:
            return PlainTextAuthProvider(username=self._username, password=self._password)
        return None

    def _cloud_config(self) -> dict | None:
        return None

    def post_connect(self) -> None:
        pass
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

client = LocalCassandraClient(
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
