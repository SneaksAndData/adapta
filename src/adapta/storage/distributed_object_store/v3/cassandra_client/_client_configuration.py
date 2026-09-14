from dataclasses import dataclass
from typing import final, Self, Any

from cassandra.cluster import DefaultConnection, _NOT_SET


@final
@dataclass
class CassandraClientConfiguration:
    reconnect_base_delay_ms: int
    reconnect_max_delay_ms: int
    socket_connection_timeout_ms: int
    socket_read_timeout_ms: int
    transient_error_max_retries: int
    transient_error_max_wait_s: int
    log_transient_errors: bool
    metadata_fetch_timeout_s: int
    protocol_version: Any
    connection_class: Any

    @classmethod
    def default(cls) -> Self:
        return cls(
            reconnect_base_delay_ms = 500,
            reconnect_max_delay_ms=5000,
            socket_connection_timeout_ms = 5000,
            socket_read_timeout_ms = 180000,
            transient_error_max_retries = 10,
            transient_error_max_wait_s = 300,
            log_transient_errors = True,
            metadata_fetch_timeout_s = 30,
            protocol_version = _NOT_SET,
            connection_class = DefaultConnection
        )