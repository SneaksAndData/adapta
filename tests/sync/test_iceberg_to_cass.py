#  Copyright (c) 2023-2026. ECCO Data & AI and other project contributors.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

from dataclasses import dataclass, field

import polars
import pytest
from cassandra.cluster import Cluster
from polars.testing import assert_frame_equal
from pyiceberg.catalog import Catalog

from adapta.logs import SemanticLogger
from adapta.logs.models import LogLevel
from adapta.process_communication import DataSocket
from adapta.storage.distributed_object_store.v3.vanilla_cassandra import VanillaCassandraClient
from adapta.storage.iceberg.v1 import write_using_catalog
from adapta.storage.sync.iceberg_to_cass import sync_iceberg_to_cassandra
from tests.iceberg_clients._functions import generate_random_string


@dataclass
class SyncItem:
    id: str = field(metadata={"is_primary_key": True, "is_partition_key": True})
    name: str
    value: int

    @classmethod
    def create_with_key_only(cls, key_value: str):
        return cls(key_value, "", 0)


@pytest.fixture(scope="module")
def cassandra_keyspace():
    cluster = Cluster(contact_points=["127.0.0.1"], port=9042)
    session = cluster.connect()
    keyspace = f"test_sync_{generate_random_string(6).lower()}"
    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {keyspace}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        AND tablets = {{ 'enabled': false }};
        """
    )
    yield keyspace
    session.execute(f"DROP KEYSPACE IF EXISTS {keyspace};")
    session.shutdown()
    cluster.shutdown()


@pytest.fixture
def cassandra_client(cassandra_keyspace: str):
    client = VanillaCassandraClient(
        client_name="test_sync_client",
        keyspace=cassandra_keyspace,
        contact_points=["127.0.0.1"],
        port=9042,
        username="cassandra",
        password="cassandra",
    )
    with client:
        yield client


@pytest.fixture
def logger():
    return SemanticLogger().add_log_source(
        log_source_name="test_sync",
        min_log_level=LogLevel.INFO,
        is_default=True,
    )


def test_sync_iceberg_to_cassandra(
    cassandra_client: VanillaCassandraClient,
    cassandra_keyspace: str,
    iceberg_catalog: Catalog,
    logger: SemanticLogger,
):
    table_suffix = generate_random_string(8).lower()
    iceberg_table_name = f"test_iceberg_{table_suffix}"
    cassandra_table_name = f"test_cass_{table_suffix}"

    # 1. Create a new iceberg table and 2. fill it with data
    initial_data = polars.DataFrame(
        {
            "id": ["1", "2", "3"],
            "name": ["alice", "bob", "charlie"],
            "value": [10, 20, 30],
        }
    )
    write_using_catalog(
        schema_name="test",
        table_name=iceberg_table_name,
        catalog=iceberg_catalog,
        data=initial_data,
        overwrite=True,
    )

    # 3. Create a new Cassandra table with matching schema
    cassandra_client.create_table(SyncItem, cassandra_table_name, cassandra_keyspace)

    iceberg_source = DataSocket(
        alias="source",
        data_path=f"iceberg://test@{iceberg_table_name}",
        data_format="iceberg",
    )
    cassandra_target = DataSocket(
        alias="target",
        data_path=f"cass+tests.sync.test_iceberg_to_cass.SyncItem://{cassandra_keyspace}@{cassandra_table_name}",
        data_format="cassandra",
    )

    # 4. Run sync
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    # 5. Update iceberg table (no schema changes)
    updated_data = polars.DataFrame(
        {
            "id": ["4", "5"],
            "name": ["david", "eve"],
            "value": [40, 50],
        }
    )
    write_using_catalog(
        schema_name="test",
        table_name=iceberg_table_name,
        catalog=iceberg_catalog,
        data=updated_data,
        overwrite=False,
    )

    # 6. Run sync
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    # 7. Validate the expected data looks as it should
    synced_records = (
        cassandra_client.get_entities_raw(f"SELECT * FROM {cassandra_keyspace}.{cassandra_table_name};")
        .to_polars()
        .sort("id")
    )
    expected_records = polars.concat([initial_data, updated_data]).sort("id")
    assert_frame_equal(synced_records, expected_records, check_column_order=False)

    # 8. Run sync again with no new changes (up to date)
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )
    synced_records_unchanged = (
        cassandra_client.get_entities_raw(f"SELECT * FROM {cassandra_keyspace}.{cassandra_table_name};")
        .to_polars()
        .sort("id")
    )
    assert_frame_equal(synced_records_unchanged, expected_records, check_column_order=False)


def test_sync_iceberg_to_cassandra_insert_update(
    cassandra_client: VanillaCassandraClient,
    cassandra_keyspace: str,
    iceberg_catalog: Catalog,
    logger: SemanticLogger,
):
    table_suffix = generate_random_string(8).lower()
    iceberg_table_name = f"test_iceberg_{table_suffix}"
    cassandra_table_name = f"test_cass_{table_suffix}"

    initial_data = polars.DataFrame(
        {
            "id": ["1", "2", "3"],
            "name": ["alice", "bob", "charlie"],
            "value": [10, 20, 30],
        }
    )
    write_using_catalog(
        schema_name="test",
        table_name=iceberg_table_name,
        catalog=iceberg_catalog,
        data=initial_data,
        overwrite=True,
    )

    cassandra_client.create_table(SyncItem, cassandra_table_name, cassandra_keyspace)

    iceberg_source = DataSocket(
        alias="source",
        data_path=f"iceberg://test@{iceberg_table_name}",
        data_format="iceberg",
    )
    cassandra_target = DataSocket(
        alias="target",
        data_path=f"cass+tests.sync.test_iceberg_to_cass.SyncItem://{cassandra_keyspace}@{cassandra_table_name}",
        data_format="cassandra",
    )

    # Initial sync
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    # Update records in Iceberg
    updated_data = polars.DataFrame(
        {
            "id": ["2", "3"],
            "name": ["bob", "charlie"],
            "value": [25, 35],
        }
    )
    write_using_catalog(
        schema_name="test",
        table_name=iceberg_table_name,
        catalog=iceberg_catalog,
        data=updated_data,
        overwrite=False,
        merge_columns=["id"],
    )

    # Run sync for update
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    synced_records = (
        cassandra_client.get_entities_raw(f"SELECT * FROM {cassandra_keyspace}.{cassandra_table_name};")
        .to_polars()
        .sort("id")
    )
    expected_records = polars.DataFrame(
        {
            "id": ["1", "2", "3"],
            "name": ["alice", "bob", "charlie"],
            "value": [10, 25, 35],
        }
    ).sort("id")
    assert_frame_equal(synced_records, expected_records, check_column_order=False)


def test_sync_iceberg_to_cassandra_insert_delete_update(
    cassandra_client: VanillaCassandraClient,
    cassandra_keyspace: str,
    iceberg_catalog: Catalog,
    logger: SemanticLogger,
):
    table_suffix = generate_random_string(8).lower()
    iceberg_table_name = f"test_iceberg_{table_suffix}"
    cassandra_table_name = f"test_cass_{table_suffix}"

    initial_data = polars.DataFrame(
        {
            "id": ["1", "2", "3"],
            "name": ["alice", "bob", "charlie"],
            "value": [10, 20, 30],
        }
    )
    write_using_catalog(
        schema_name="test",
        table_name=iceberg_table_name,
        catalog=iceberg_catalog,
        data=initial_data,
        overwrite=True,
    )

    cassandra_client.create_table(SyncItem, cassandra_table_name, cassandra_keyspace)

    iceberg_source = DataSocket(
        alias="source",
        data_path=f"iceberg://test@{iceberg_table_name}",
        data_format="iceberg",
    )
    cassandra_target = DataSocket(
        alias="target",
        data_path=f"cass+tests.sync.test_iceberg_to_cass.SyncItem://{cassandra_keyspace}@{cassandra_table_name}",
        data_format="cassandra",
    )

    # 1. Initial sync
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    # 2. Delete record from Iceberg
    iceberg_table = iceberg_catalog.load_table(identifier=("test", iceberg_table_name))
    iceberg_table.delete("id = '2'")

    # Run sync after delete
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    synced_records_after_delete = (
        cassandra_client.get_entities_raw(f"SELECT * FROM {cassandra_keyspace}.{cassandra_table_name};")
        .to_polars()
        .sort("id")
    )
    expected_after_delete = polars.DataFrame(
        {
            "id": ["1", "3"],
            "name": ["alice", "charlie"],
            "value": [10, 30],
        }
    ).sort("id")
    assert_frame_equal(synced_records_after_delete, expected_after_delete, check_column_order=False)

    # 3. Update record in Iceberg
    updated_data = polars.DataFrame(
        {
            "id": ["3"],
            "name": ["charlie"],
            "value": [35],
        }
    )
    write_using_catalog(
        schema_name="test",
        table_name=iceberg_table_name,
        catalog=iceberg_catalog,
        data=updated_data,
        overwrite=False,
        merge_columns=["id"],
    )

    # Run sync after update
    sync_iceberg_to_cassandra(
        iceberg_catalog=iceberg_catalog,
        client=cassandra_client,
        iceberg_source=iceberg_source,
        version_field="value",
        cassandra_target=cassandra_target,
        chunk_size=2,
        logger=logger,
    )

    synced_records_after_update = (
        cassandra_client.get_entities_raw(f"SELECT * FROM {cassandra_keyspace}.{cassandra_table_name};")
        .to_polars()
        .sort("id")
    )
    expected_after_update = polars.DataFrame(
        {
            "id": ["1", "3"],
            "name": ["alice", "charlie"],
            "value": [10, 35],
        }
    ).sort("id")
    assert_frame_equal(synced_records_after_update, expected_after_update, check_column_order=False)
