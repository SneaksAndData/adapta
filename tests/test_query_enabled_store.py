from pydoc import locate
from typing import Optional, Type, Union

import pytest

from adapta.storage.query_enabled_store import QueryEnabledStore, DeltaQueryEnabledStore, AstraQueryEnabledStore


@pytest.mark.parametrize(
    "connection_string, expected_store_type",
    [
        (
            'qes://engine=adapta.storage.query_enabled_store.DeltaQueryEnabledStore;plaintext_credentials={"auth_client_class":"adapta.security.clients.AzureClient"};settings={}',
            DeltaQueryEnabledStore,
        ),
        (
            'qes://engine=DELTA;plaintext_credentials={"auth_client_class":"adapta.security.clients.AzureClient"};settings={}',
            DeltaQueryEnabledStore,
        ),
        (
            'qes://engine=DELTA;plaintext_credentials={"auth_client_class":"adapta.security.clients.aws.AwsClient"};settings={}',
            DeltaQueryEnabledStore,
        ),
        (
            'qes://engine=DELTA;plaintext_credentials={"auth_client_class":"adapta.security.clients.aws.AwsClient", "auth_client_credentials_class": "adapta.security.clients.aws._aws_credentials.ExplicitAwsCredentials", "access_key": "test", "access_key_id": "test", "region": "test", "endpoint": "https://test"};settings={}',
            DeltaQueryEnabledStore,
        ),
        (
            'qes://engine=DELTA;plaintext_credentials={"auth_client_class":"adapta.security.clients.aws.AwsClient", "auth_client_credentials_class": "adapta.security.clients.aws._aws_credentials.EnvironmentAwsCredentials"};settings={}',
            DeltaQueryEnabledStore,
        ),
        (
            'qes://engine=DELTA;plaintext_credentials={"auth_client_class":"adapta.security.clients.TestClient"};settings={}',
            ModuleNotFoundError,
        ),
        (
            'qes://engine=ASTRA;plaintext_credentials={"secret_connection_bundle_bytes":"test", "client_id": "test", "client_secret": "test"};settings={"keyspace": "tmp"}',
            AstraQueryEnabledStore,
        ),
        (
            'qes://engine=adapta.storage.query_enabled_store.AstraQueryEnabledStore;plaintext_credentials={"secret_connection_bundle_bytes":"test", "client_id": "test", "client_secret": "test"};settings={"keyspace": "tmp"}',
            AstraQueryEnabledStore,
        ),
        (
            'qes://engine=adapta.storage.query_enabled_store.AstraQueryEnabledStore;plaintext_credentials={"secret_connection_bundle_bytes":"test", "client_id": "test", "client_secret": "test"};settings={"client_name": "test", "keyspace": "tmp"}',
            AstraQueryEnabledStore,
        ),
        (
            'qes://engine=adapta.storage.query_enabled_store.AstraQueryEnabledStore;plaintext_credentials={};settings={"client_name": "test", "keyspace": "tmp"}',
            RuntimeError,
        ),
        (
            'qes://engine=DELT;plaintext_credentials={"auth_client_class":"adapta.security.clients.AzureClient"};settings={}',
            ModuleNotFoundError,
        ),
    ],
)
def test_query_store_instantiation(
    connection_string: str, expected_store_type: Union[Type[QueryEnabledStore], Exception]
):
    try:
        store = QueryEnabledStore.from_string(connection_string, lazy_init=True)
        assert isinstance(store, expected_store_type)
        if expected_store_type == DeltaQueryEnabledStore:
            credentials_client = locate(store.credentials.auth_client_class)(**store.credentials.to_dict())
            assert credentials_client is not None, "Credentials client could not be loaded"
    except Exception as load_error:
        assert isinstance(load_error, expected_store_type)
