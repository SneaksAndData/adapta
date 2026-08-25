from adapta.process_communication import DataSocket
from adapta.storage.models import (
    parse_data_path,
    IcebergPath,
    LocalPath,
    AstraPath,
    S3Path,
    WasbPath,
    AdlsGen2Path,
    TrinoPath,
)
from adapta.storage.query_enabled_store._qes_astra import AstraQueryEnabledStore
from adapta.storage.query_enabled_store._models import QueryEnabledStore
from adapta.storage.query_enabled_store._qes_iceberg import IcebergQueryEnabledStore
from adapta.storage.query_enabled_store._qes_local import LocalQueryEnabledStore
from adapta.storage.query_enabled_store._qes_delta import DeltaQueryEnabledStore
from adapta.storage.query_enabled_store._qes_trino import TrinoQueryEnabledStore


def suggest_store(socket: DataSocket) -> type[QueryEnabledStore] | None:
    """
    Suggest supported store implementation to use with the provided socket format
    """
    path_impl = parse_data_path(socket.data_path)
    if isinstance(path_impl, IcebergPath):
        return IcebergQueryEnabledStore
    if isinstance(path_impl, LocalPath):
        return LocalQueryEnabledStore
    if isinstance(path_impl, AstraPath):
        return AstraQueryEnabledStore
    if isinstance(path_impl, (S3Path, WasbPath, AdlsGen2Path)):
        return DeltaQueryEnabledStore
    if isinstance(path_impl, TrinoPath):
        return TrinoQueryEnabledStore

    return None
