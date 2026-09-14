from dataclasses import dataclass
from typing import final, Self, Any

from cassandra.cluster import DefaultConnection, _NOT_SET


@final
@dataclass
class CassandraClientConfiguration:
    """
     Advanced configuration parameters for Cassandra clients

     reconnect_base_delay_ms: Reconnect delay in ms, in case of a connection or node failure (min value for exp. backoff).
     reconnect_max_delay_ms: Reconnect delay in ms, in case of a connection or node failure (max value for exp backoff).
     socket_connection_timeout: Connect timeout for the TCP connection.
     socket_read_timeout: Read timeout for TCP operations (query timeout).
     transient_error_max_retries: Maximum number of exp backoff retries for transient errors like rate limit.
     transient_error_max_wait_s: Maximum cumulative wait time for exp backoff attempts for transient errors.
     log_transient_errors: Whether to log errors that can be resolved via exp backoff retries.
     metadata_fetch_timeout_s: Timeout in seconds for the driver’s HTTP call to get cluster metadata from Astra DB. Defaults to 30s up fromf factory default of 5 seconds.
     protocol_version: Cassandra protocol version to use. Defaults to the latest version supported by the driver.

    """
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