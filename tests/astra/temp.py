import os

from adapta.process_communication import DataSocket
from adapta.security.clients import HashicorpVaultOidcClient
from adapta.storage.distributed_object_store.v3.datastax_astra import AstraClient

from dataclasses import dataclass, field

from adapta.storage.models import DataPath
from adapta.storage.models.filter_expression import FilterField
from adapta.storage.query_enabled_store import QueryEnabledStore
from adapta.storage.secrets.hashicorp_vault_secret_storage_client import HashicorpSecretStorageClient


@dataclass
class TestEntity:

    column_a: str = field(
        metadata={
            "is_primary_key": True,
            "is_partition_key": True,
        }
    )
    column_b: list[str]
    column_c: list[dict[str, float]]

rows_to_insert = [
    {'column_a': '1', 'column_b': [], 'column_c': [{'key_a': 1, 'key_b': 2}]},
    {'column_a': '2', 'column_b': ['1', '3', '4'], 'column_c': [{'key_a': 1, 'key_b': 2}, {'key_a': 3, 'key_b': 4}]}
]


secrets_client = HashicorpSecretStorageClient(
        base_client=HashicorpVaultOidcClient("https://hashicorp-vault.production.sneaksanddata.com/")
    )
secrets = {
    **secrets_client.read_secret(
        storage_name="secret",
        secret_name="applications/astra/algorithms-aws-nosql-production-0/secure-connection-bundle",
    ),
    **secrets_client.read_secret(
        storage_name="secret",
        secret_name="applications/astra/algorithms-aws-nosql-production-0/role-credentials",
    ),
}

os.environ["PROTEUS__ASTRA_BUNDLE_BYTES"] = secrets["ASTRA_BUNDLE"]
os.environ["PROTEUS__ASTRA_CLIENT_ID"] = secrets["client_id"]
os.environ["PROTEUS__ASTRA_CLIENT_SECRET"] = secrets["secret"]
os.environ["CRYSTAL__ASTRA_BUNDLE"] = secrets["ASTRA_BUNDLE"]
os.environ["CRYSTAL__ASTRA_CLIENT_ID"] = secrets["client_id"]
os.environ["CRYSTAL__ASTRA_CLIENT_SECRET"] = secrets["secret"]
os.environ["CRYSTAL__ASTRA_KEYSPACE"] = "auto_replenishment"
query_enabled_store = QueryEnabledStore.from_string('qes://engine=ASTRA;plaintext_credentials={};settings={"client_name":"auto_replenishment_e2e", "keyspace": "auto_replenishment"}')

socket = DataSocket("location", "astra+real_time_algorithm_data_models.RealTimeAlgorithmLocation://auto_replenishment@location_temp", 'astra')

filter_ = (FilterField("location_key") == 'test')


columns = ['location_key']

# This should work:
location_1 = query_enabled_store.open(socket.parse_data_path()).filter(filter_).select(*columns).read().to_polars()

