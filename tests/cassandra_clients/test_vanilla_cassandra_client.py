from dataclasses import dataclass, field

import polars
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
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS test_vanilla.test_item_0 (
            id text PRIMARY KEY,
            name text,
            value int
        );
        """
    )
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS test_vanilla.test_item_1 (
            id text PRIMARY KEY,
            name text,
            value int
        );
        """
    )
    session.execute("TRUNCATE test_vanilla.test_item;")
    session.execute("TRUNCATE test_vanilla.test_item_0;")
    session.execute("TRUNCATE test_vanilla.test_item_1;")

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

        all_polars = client.filter_entities(
            model_class=VanillaItem,
            table_name="test_item",
            key_column_filter_values=[],
            select_columns=["id", "name"],
        ).to_polars()
        assert len(all_polars) == 3

        all_polars_empty_cols = client.filter_entities(
            model_class=VanillaItem,
            table_name="test_item",
            key_column_filter_values=[],
            select_columns=[],
        ).to_polars()
        assert len(all_polars_empty_cols) == 3
        assert set(all_polars_empty_cols.columns) == {"id", "name", "value"}

        all_polars_none_cols = client.filter_entities(
            model_class=VanillaItem,
            table_name="test_item",
            key_column_filter_values=[],
            select_columns=None,
        ).to_polars()
        assert len(all_polars_none_cols) == 3
        assert set(all_polars_none_cols.columns) == {"id", "name", "value"}

        client.delete_entity(entity=item, table_name="test_item")
        after_delete = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;").to_pandas()
        assert len(after_delete) == 2
        assert "item-1" not in set(after_delete["id"])

        # Test upload_concurrent_native
        native_data = polars.DataFrame(
            [
                {"id": "item-4", "name": "fourth", "value": 400},
                {"id": "item-5", "name": "fifth", "value": 500},
            ]
        )
        upserted_native = client.upload_concurrent_native(
            source=native_data.lazy(),
            table_name="test_item",
            entity_type=VanillaItem,
            batch_size=2,
            threads=2,
        )
        assert upserted_native == 2

        after_native = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;").to_pandas()
        assert len(after_native) == 4
        assert {"item-4", "item-5"}.issubset(set(after_native["id"]))

        # Test upload_concurrent_batch
        batch_data = polars.DataFrame(
            [
                {"id": "item-6", "name": "sixth", "value": 600},
                {"id": "item-7", "name": "seventh", "value": 700},
            ]
        )
        upserted_batch = client.upload_concurrent_batch(
            source=batch_data.lazy(),
            table_name="test_item",
            entity_type=VanillaItem,
            batch_size=2,
            threads=2,
        )
        assert upserted_batch == 2

        after_batch = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;").to_pandas()
        assert len(after_batch) == 6
        assert {"item-6", "item-7"}.issubset(set(after_batch["id"]))

        # Test delete_batch_concurrent
        deletes_data = polars.DataFrame(
            [
                {"id": "item-6", "name": "sixth", "value": 600},
                {"id": "item-7", "name": "seventh", "value": 700},
            ]
        )
        deleted_count = client.delete_batch_concurrent(
            source=deletes_data.lazy(),
            table_name="test_item",
            entity_type=VanillaItem,
            batch_size=2,
            threads=2,
        )
        assert deleted_count == 2

        after_concurrent_delete = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;").to_pandas()
        assert len(after_concurrent_delete) == 4
        assert "item-6" not in set(after_concurrent_delete["id"])
        assert "item-7" not in set(after_concurrent_delete["id"])

        # Test direct upsert_concurrent with LazyFrame
        direct_lf_data = polars.DataFrame(
            [
                {"id": "item-8", "name": "eighth", "value": 800},
            ]
        ).lazy()
        client.upsert_concurrent(
            rows=direct_lf_data,
            table_name="test_item",
            entity_type=VanillaItem,
        )
        after_lf_upsert = client.get_entities_raw("SELECT * FROM test_vanilla.test_item;").to_pandas()
        assert "item-8" in set(after_lf_upsert["id"])

        # Test empty LazyFrame and empty list handling
        client.upsert_concurrent(
            rows=polars.LazyFrame(schema={"id": polars.String, "name": polars.String, "value": polars.Int32}),
            table_name="test_item",
            entity_type=VanillaItem,
        )
        client.upsert_concurrent(
            rows=[],
            table_name="test_item",
            entity_type=VanillaItem,
        )

        # Test upload_concurrent_native with shard_count
        sharded_native_data = polars.DataFrame(
            [
                {"id": "item-9", "name": "ninth", "value": 900, "rowhash": "00000000"},
                {"id": "item-10", "name": "tenth", "value": 1000, "rowhash": "00000001"},
                {"id": "item-11", "name": "eleventh", "value": 1100, "rowhash": "00000002"},
            ]
        )
        upserted_sharded_native = client.upload_concurrent_native(
            source=sharded_native_data.lazy(),
            table_name="test_item",
            entity_type=VanillaItem,
            batch_size=2,
            threads=2,
            shard_count=2,
        )
        assert upserted_sharded_native == 3

        # Test upload_concurrent_batch with shard_count
        sharded_batch_data = polars.DataFrame(
            [
                {"id": "item-12", "name": "twelfth", "value": 1200, "rowhash": "00000004"},
                {"id": "item-13", "name": "thirteenth", "value": 1300, "rowhash": "00000005"},
            ]
        )
        upserted_sharded_batch = client.upload_concurrent_batch(
            source=sharded_batch_data.lazy(),
            table_name="test_item",
            entity_type=VanillaItem,
            batch_size=2,
            threads=2,
            shard_count=2,
        )
        assert upserted_sharded_batch == 2

        after_shard_0 = client.get_entities_raw("SELECT * FROM test_vanilla.test_item_0;").to_pandas()
        assert set(after_shard_0["id"]) == {"item-9", "item-11", "item-12"}

        after_shard_1 = client.get_entities_raw("SELECT * FROM test_vanilla.test_item_1;").to_pandas()
        assert set(after_shard_1["id"]) == {"item-10", "item-13"}

        # Test delete_batch_concurrent with shard_count (deletes PKs from all shard tables, rowhash not required)
        sharded_deletes_data = polars.DataFrame(
            [
                {"id": "item-9", "name": "ninth", "value": 900},
                {"id": "item-10", "name": "tenth", "value": 1000},
            ]
        )
        deleted_sharded_count = client.delete_batch_concurrent(
            source=sharded_deletes_data.lazy(),
            table_name="test_item",
            entity_type=VanillaItem,
            batch_size=2,
            threads=2,
            shard_count=2,
        )
        assert deleted_sharded_count == 4

        after_delete_shard_0 = client.get_entities_raw("SELECT * FROM test_vanilla.test_item_0;").to_pandas()
        assert set(after_delete_shard_0["id"]) == {"item-11", "item-12"}

        after_delete_shard_1 = client.get_entities_raw("SELECT * FROM test_vanilla.test_item_1;").to_pandas()
        assert set(after_delete_shard_1["id"]) == {"item-13"}

        # Test filter_entities with shard_count
        sharded_filtered = client.filter_entities(
            model_class=VanillaItem,
            table_name="test_item",
            key_column_filter_values=[],
            select_columns=["id", "name"],
            shard_count=2,
        ).to_pandas()
        assert set(sharded_filtered["id"]) == {"item-11", "item-12", "item-13"}

        sharded_filtered_single = client.filter_entities(
            model_class=VanillaItem,
            table_name="test_item",
            key_column_filter_values=[{"id": "item-13"}],
            select_columns=["id", "name"],
            shard_count=2,
        ).to_pandas()
        assert len(sharded_filtered_single) == 1
        assert sharded_filtered_single.iloc[0]["id"] == "item-13"


def test_prepare_lazy_source_converts_list_struct_to_map():
    from adapta.storage.distributed_object_store.v3.cassandra_client._client import _prepare_lazy_source

    df_list_struct = polars.DataFrame(
        {
            "id": ["1", "2"],
            "metadata": [
                [{"key": "env", "value": "prod"}, {"key": "region", "value": "eu"}],
                [{"key": "env", "value": "dev"}, {"key": "region", "value": "us"}],
            ],
        }
    )
    prepared = _prepare_lazy_source(df_list_struct)
    assert isinstance(prepared.collect_schema()["metadata"], polars.Map)
    assert prepared.collect().to_dicts() == [
        {"id": "1", "metadata": {"env": "prod", "region": "eu"}},
        {"id": "2", "metadata": {"env": "dev", "region": "us"}},
    ]

    # DataFrame with no map structures remains untouched
    df_regular = polars.DataFrame({"id": ["1", "2"], "val": [10, 20]})
    prepared_regular = _prepare_lazy_source(df_regular)
    assert prepared_regular.collect_schema() == df_regular.schema


def test_shard_source():
    from adapta.storage.distributed_object_store.v3.cassandra_client._client import _shard_source

    # Hexstring rowhash
    df_hex = polars.DataFrame(
        {
            "id": ["1", "2", "3", "4"],
            "rowhash": [
                "c87cbaa39f7c0a31967508cc48bd4260",
                "c87cbaa49f7c0a31967508cc48bd4260",
                "000000001234567890abcdef12345678",
                "000000011234567890abcdef12345678",
            ],
        }
    )
    shards = _shard_source(df_hex.lazy(), shard_count=2)
    assert len(shards) == 2
    s0 = shards[0].collect()
    s1 = shards[1].collect()
    assert len(s0) + len(s1) == 4
    assert "__bucket" not in s0.columns
    assert "__bucket" not in s1.columns

    # Missing rowhash raises ValueError
    df_no_hash = polars.DataFrame({"id": ["1", "2"]})
    with pytest.raises(ValueError, match="A 'rowhash' column is required"):
        _shard_source(df_no_hash.lazy(), shard_count=2)


def test_upload_concurrent_shard_validation(setup_keyspace_and_table):
    client = VanillaCassandraClient(
        client_name="test_vanilla_client",
        keyspace="test_vanilla",
        contact_points=["127.0.0.1"],
        port=9042,
        username="cassandra",
        password="cassandra",
    )

    with client:
        df_no_hash = polars.DataFrame([{"id": "item-err", "name": "err", "value": 1}])

        # Missing rowhash when shard_count > 1
        with pytest.raises(ValueError, match="A 'rowhash' column is required"):
            client.upload_concurrent_native(
                source=df_no_hash.lazy(),
                table_name="test_item",
                entity_type=VanillaItem,
                shard_count=2,
            )

        with pytest.raises(ValueError, match="A 'rowhash' column is required"):
            client.upload_concurrent_batch(
                source=df_no_hash.lazy(),
                table_name="test_item",
                entity_type=VanillaItem,
                shard_count=2,
            )

        # Invalid shard_count <= 0
        with pytest.raises(ValueError, match="shard_count must be greater than 0"):
            client.upload_concurrent_native(
                source=df_no_hash.lazy(),
                table_name="test_item",
                entity_type=VanillaItem,
                shard_count=0,
            )

        with pytest.raises(ValueError, match="shard_count must be greater than 0"):
            client.upload_concurrent_batch(
                source=df_no_hash.lazy(),
                table_name="test_item",
                entity_type=VanillaItem,
                shard_count=-1,
            )

        with pytest.raises(ValueError, match="shard_count must be greater than 0"):
            client.delete_batch_concurrent(
                source=df_no_hash.lazy(),
                table_name="test_item",
                entity_type=VanillaItem,
                shard_count=0,
            )


def test_cassandra_qes_sharded_read(setup_keyspace_and_table):
    from adapta.storage.models.cassandra import CassandraPath
    from adapta.storage.query_enabled_store import QueryEnabledStore, QueryEnabledStoreMode
    from adapta.storage.query_enabled_store.parameters import (
        QueryEnabledStoreFilterParameter,
        QueryEnabledStoreShardCountParameter,
    )

    client = VanillaCassandraClient(
        client_name="test_vanilla_client",
        keyspace="test_vanilla",
        contact_points=["127.0.0.1"],
        port=9042,
        username="cassandra",
        password="cassandra",
    )
    with client:
        client.upsert_batch(
            entities=[
                {"id": "qes-1", "name": "one", "value": 10},
                {"id": "qes-2", "name": "two", "value": 20},
            ],
            entity_type=VanillaItem,
            table_name="test_item_0",
        )
        client.upsert_batch(
            entities=[
                {"id": "qes-3", "name": "three", "value": 30},
            ],
            entity_type=VanillaItem,
            table_name="test_item_1",
        )

    connection_string = (
        'qes://engine=CASSANDRA;plaintext_credentials={"username": "cassandra", "password": "cassandra"};'
        'settings={"keyspace": "test_vanilla", "client_name": "test_vanilla_client", "contact_points": ["127.0.0.1"]}'
    )
    store = QueryEnabledStore.from_string(connection_string, lazy_init=True)
    path = CassandraPath(
        keyspace="test_vanilla",
        table="test_item",
        model_class_name="tests.cassandra_clients.test_vanilla_cassandra_client.VanillaItem",
    )

    # Read all rows across shards as a single table
    result = (
        store.open(path, QueryEnabledStoreMode.READ).set_parameters(QueryEnabledStoreShardCountParameter(2)).execute()
    )
    df = result.to_pandas()
    assert {"qes-1", "qes-2", "qes-3"}.issubset(set(df["id"]))

    # Read with filter across shards
    filtered_result = (
        store.open(path, QueryEnabledStoreMode.READ)
        .set_parameters(
            QueryEnabledStoreFilterParameter([{"id": "qes-3"}]),
            QueryEnabledStoreShardCountParameter(2),
        )
        .execute()
    )
    df_filtered = filtered_result.to_pandas()
    assert len(df_filtered) == 1
    assert df_filtered.iloc[0]["id"] == "qes-3"
