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


try:
    from adapta.storage.query_enabled_store._qes_astra import AstraQueryEnabledStore
except (ImportError, ModuleNotFoundError) as ex:
    print(f"Query Enabled Store (Astra) not configured: {ex}")

try:
    from adapta.storage.query_enabled_store._qes_iceberg import IcebergQueryEnabledStore
except (ImportError, ModuleNotFoundError) as ex:
    print(f"Query Enabled Store (Iceberg) not configured: {ex}")

try:
    from adapta.storage.query_enabled_store._qes_delta import DeltaQueryEnabledStore
except (ImportError, ModuleNotFoundError) as ex:
    print(f"Query Enabled Store (Delta) not configured: {ex}")

try:
    from adapta.storage.query_enabled_store._qes_trino import TrinoQueryEnabledStore
except (ImportError, ModuleNotFoundError) as ex:
    print(f"Query Enabled Store (Trino) not configured: {ex}")

from adapta.storage.query_enabled_store._qes_local import LocalQueryEnabledStore
from adapta.storage.query_enabled_store._models import QueryEnabledStore


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
