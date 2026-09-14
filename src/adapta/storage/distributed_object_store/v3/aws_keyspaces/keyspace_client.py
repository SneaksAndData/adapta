import ssl
from pathlib import Path
from typing import final

import boto3
from cassandra.auth import AuthProvider
from cassandra_sigv4.auth import SigV4AuthProvider

from adapta.storage.distributed_object_store.v3.cassandra_client import CassandraClient, CassandraClientConfiguration


@final
class AwsKeyspaceClient(CassandraClient):
    """
    Cassandra client for AWS Keyspaces. Supports SigV4 (IAM) auth.
    """

    def __init__(
        self, keyspace: str, client_name: str, region: str, client_config: CassandraClientConfiguration | None = None
    ) -> None:
        super().__init__(client_name, keyspace, client_config)
        self._region = region

    def _get_port(self) -> int | None:
        return 9142

    def _get_contact_points(self) -> list[str] | None:
        return [f"cassandra.{self._region}.amazonaws.com"]

    def _get_ssl_context(self) -> ssl.SSLContext | None:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        public_certificate_location = Path(__file__).parent / "keyspaces-certificate.pem"
        ssl_context.load_verify_locations(public_certificate_location)
        ssl_context.verify_mode = ssl.CERT_REQUIRED

        return ssl_context

    def post_connect(self) -> None:
        return None

    def _cloud_config(self) -> dict | None:
        return None

    def _get_auth_provider(self) -> AuthProvider:
        session = boto3.Session(
            region_name=self._region,
        )
        return SigV4AuthProvider(session=session)
