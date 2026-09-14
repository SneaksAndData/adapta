import ssl
from typing import final

from cassandra.auth import AuthProvider, PlainTextAuthProvider

from adapta.storage.distributed_object_store.v3.cassandra_client import (
    CassandraClient,
    CassandraClientConfiguration,
)


@final
class VanillaCassandraClient(CassandraClient):
    """
    Standard / local Cassandra client.
    """

    def __init__(
        self,
        client_name: str,
        keyspace: str,
        username: str,
        password: str,
        contact_points: list[str] | None = None,
        port: int = 9042,
        ssl_context: ssl.SSLContext | None = None,
        client_config: CassandraClientConfiguration | None = None,
    ) -> None:
        super().__init__(client_name, keyspace, client_config)
        self._contact_points = contact_points or ["127.0.0.1"]
        self._port = port
        self._username = username
        self._password = password
        self._ssl_context = ssl_context

    def _get_port(self) -> int | None:
        return self._port

    def _get_contact_points(self) -> list[str] | None:
        return self._contact_points

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        return self._ssl_context

    def _get_auth_provider(self) -> AuthProvider:
        return PlainTextAuthProvider(username=self._username, password=self._password)

    def _cloud_config(self) -> dict | None:
        return None

    def post_connect(self) -> None:
        return None
