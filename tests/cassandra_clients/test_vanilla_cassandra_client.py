from dataclasses import dataclass, field

import pytest
from cassandra.cluster import Cluster

from adapta.storage.distributed_object_store.v3.vanilla_cassandra import (
    VanillaCassandraClient,
)


@dataclass
class VanillaItem:
    id: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    name: str
    value: int


@pytest.fixture(scope="module")
def setup_keyspace_and_table():
    cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
    session = cluster.connect()

    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS test_vanilla
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        AND tablets = { 'enabled': false };
        """
    )
    session.set_keyspace("test_vanilla")
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS test_vanilla.test_item (
            id text PRIMARY KEY,
            name text,
            value int
        );
        """
    )
    session.execute("TRUNCATE test_vanilla.test_item;")

    yield

    session.execute("DROP KEYSPACE IF EXISTS test_vanilla;")
    session.shutdown()
    cluster.shutdown()


def test_vanilla_cassandra_client_connect_and_crud(setup_keyspace_and_table):
    client = VanillaCassandraClient(
        client_name="test_vanilla_client",
        keyspace="test_vanilla",
        contact_points=["127.0.0.1"],
        port=9042,
        username="cassandra",
        password="cassandra",
    )

    with client:
        item = VanillaItem(id="item-1", name="first", value=42)
        client.upsert_entity(entity=item, table_name="test_item")

        read_single = client.get_entity("test_item")
        assert read_single["id"] == "item-1"
        assert read_single["name"] == "first"
        assert read_single["value"] == 42

        batch = [
            {"id": "item-2", "name": "second", "value": 100},
            {"id": "item-3", "name": "third", "value": 200},
        ]
        client.upsert_batch(
            entities=batch,
            entity_type=VanillaItem,
            table_name="test_item",
        )

        meta_frame = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;")
        df_pandas = meta_frame.to_pandas()
        assert len(df_pandas) == 3
        assert set(df_pandas["id"]) == {"item-1", "item-2", "item-3"}

        filtered = client.filter_entities(
            model_class=VanillaItem,
            table_name="test_item",
            key_column_filter_values=[{"id": "item-2"}],
            select_columns=["id", "name"],
        ).to_pandas()
        assert len(filtered) == 1
        assert filtered.iloc[0]["id"] == "item-2"
        assert filtered.iloc[0]["name"] == "second"

        client.delete_entity(entity=item, table_name="test_item")
        after_delete = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;").to_pandas()
        assert len(after_delete) == 2
        assert "item-1" not in set(after_delete["id"])
