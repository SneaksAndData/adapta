import ssl
from typing import final

import boto3
from cassandra.cluster import Cluster
from cassandra.cqlengine.connection import set_session
from cassandra_sigv4.auth import SigV4AuthProvider

from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient, CassandraClientConfiguration


@final
class AwsKeyspaceClient(CassandraClient):
    def __init__(self, keyspace: str, client_name: str, region: str, client_config: CassandraClientConfiguration | None = None) -> None:
        super().__init__(client_name, keyspace, client_config)
        self._region = region

    def connect(self) -> None:
        session = boto3.Session(
            region_name=self._region,
        )
        auth_provider = SigV4AuthProvider(session=session)
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.load_verify_locations('AmazonRootCA1.pem')
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        self._cluster = Cluster(
            [f"cassandra.{self._region}.amazonaws.com"],
            port=9142,
            auth_provider=auth_provider,
            ssl_context=ssl_context
        )
        self._session = self._cluster.connect(keyspace=self._keyspace)

        set_session(self._session)


    async def connect_async(self) -> None:
        pass